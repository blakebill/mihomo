package dialfeedback

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

const defaultCapacity = 256

// Event is a privacy-preserving Smart candidate dial outcome. It intentionally
// excludes the destination host, IP address, port, and raw error text.
type Event struct {
	Sequence   uint64 `json:"sequence"`
	Group      string `json:"group,omitempty"`
	Outbound   string `json:"outbound"`
	Network    string `json:"network,omitempty"`
	Signal     string `json:"signal"`
	Success    bool   `json:"success"`
	DurationMs int64  `json:"durationMs"`
	Timestamp  int64  `json:"timestamp"`
	ErrorClass string `json:"errorClass"`
}

type Snapshot struct {
	Instance string  `json:"instance"`
	Sequence uint64  `json:"sequence"`
	Events   []Event `json:"events"`
}

type eventRing struct {
	events []Event
	start  int
	count  int
}

func newEventRing(capacity int) eventRing {
	return eventRing{events: make([]Event, capacity)}
}

func (r *eventRing) add(event Event) {
	index := (r.start + r.count) % len(r.events)
	if r.count == len(r.events) {
		index = r.start
		r.start = (r.start + 1) % len(r.events)
	} else {
		r.count++
	}
	r.events[index] = event
}

func (r *eventRing) snapshotSince(since uint64) []Event {
	eventCount := 0
	for index := 0; index < r.count; index++ {
		if r.events[(r.start+index)%len(r.events)].Sequence > since {
			eventCount++
		}
	}
	events := make([]Event, 0, eventCount)
	for index := 0; index < r.count; index++ {
		event := r.events[(r.start+index)%len(r.events)]
		if event.Sequence > since {
			events = append(events, event)
		}
	}
	return events
}

// Store keeps independent fixed-size detailed and legacy rings. Supplemental
// stage signals cannot reduce the original GUI's 256-attempt retention.
type Store struct {
	mu       sync.RWMutex
	detailed eventRing
	legacy   eventRing
	sequence uint64
	instance string
}

func NewStore(capacity int) *Store {
	if capacity < 1 {
		capacity = 1
	}
	return &Store{
		detailed: newEventRing(capacity),
		legacy:   newEventRing(capacity),
		instance: newInstanceID(),
	}
}

var Default = NewStore(defaultCapacity)

func newInstanceID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("dial feedback instance ID: " + err.Error())
	}
	return hex.EncodeToString(value[:])
}

func (s *Store) Add(group, outbound, network string, success bool, duration time.Duration, errorClass string) {
	s.add(group, outbound, network, "", success, duration, duration, errorClass, true)
}

func (s *Store) AddLegacySignal(group, outbound, network, signal string, success bool, duration time.Duration, errorClass string) {
	s.add(group, outbound, network, signal, success, duration, duration, errorClass, true)
}

func (s *Store) AddLegacySignalDurations(
	group, outbound, network, signal string,
	success bool,
	detailedDuration, legacyDuration time.Duration,
	errorClass string,
) {
	s.add(group, outbound, network, signal, success, detailedDuration, legacyDuration, errorClass, true)
}

func (s *Store) AddSupplementalSignal(group, outbound, network, signal string, success bool, duration time.Duration, errorClass string) {
	s.add(group, outbound, network, signal, success, duration, duration, errorClass, false)
}

func (s *Store) add(
	group, outbound, network, signal string,
	success bool,
	detailedDuration, legacyDuration time.Duration,
	errorClass string,
	legacy bool,
) {
	if s == nil || outbound == "" {
		return
	}
	event := Event{
		Group:      group,
		Outbound:   outbound,
		Network:    network,
		Signal:     normalizeSignal(signal, network),
		Success:    success,
		DurationMs: durationMillis(detailedDuration),
		Timestamp:  time.Now().UnixMilli(),
		ErrorClass: normalizeErrorClass(success, errorClass),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	event.Sequence = s.sequence
	s.detailed.add(event)
	if legacy {
		event.DurationMs = durationMillis(legacyDuration)
		s.legacy.add(event)
	}
}

func (s *Store) SnapshotSince(since uint64) Snapshot {
	return s.snapshotSince(since, true)
}

// SnapshotLegacySince is an explicit alias for the original exported API.
func (s *Store) SnapshotLegacySince(since uint64) Snapshot {
	return s.SnapshotSince(since)
}

// SnapshotDetailedSince returns the opt-in stage-specific outcome stream.
func (s *Store) SnapshotDetailedSince(since uint64) Snapshot {
	return s.snapshotSince(since, false)
}

func (s *Store) snapshotSince(since uint64, legacyOnly bool) Snapshot {
	if s == nil {
		return Snapshot{Events: []Event{}}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if since >= s.sequence {
		return Snapshot{Instance: s.instance, Sequence: s.sequence, Events: []Event{}}
	}
	ring := &s.detailed
	if legacyOnly {
		ring = &s.legacy
	}
	return Snapshot{Instance: s.instance, Sequence: s.sequence, Events: ring.snapshotSince(since)}
}

func durationMillis(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	durationMs := int64((duration + 500*time.Microsecond) / time.Millisecond)
	if durationMs < 1 {
		return 1
	}
	return durationMs
}

func normalizeSignal(signal, network string) string {
	switch signal {
	case "tcp", "udp", "handshake", "first-byte":
		return signal
	}
	if strings.EqualFold(network, "udp") {
		return "udp"
	}
	return "tcp"
}

func normalizeErrorClass(success bool, errorClass string) string {
	if success {
		return ""
	}
	switch errorClass {
	case "canceled", "network", "soft-fail", "timeout":
		return errorClass
	default:
		return "unknown"
	}
}

// ErrorClass reduces a dial error to a small stable category without retaining
// its text, which may contain a destination address.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "network"
}
