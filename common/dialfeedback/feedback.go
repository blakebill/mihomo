package dialfeedback

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
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

// Store is a fixed-size, concurrency-safe circular event buffer.
type Store struct {
	mu       sync.RWMutex
	events   []Event
	start    int
	count    int
	sequence uint64
	instance string
}

func NewStore(capacity int) *Store {
	if capacity < 1 {
		capacity = 1
	}
	return &Store{
		events:   make([]Event, capacity),
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
	if s == nil || outbound == "" {
		return
	}
	var durationMs int64
	if duration > 0 {
		durationMs = int64((duration + 500*time.Microsecond) / time.Millisecond)
		if durationMs < 1 {
			durationMs = 1
		}
	}
	event := Event{
		Group:      group,
		Outbound:   outbound,
		Network:    network,
		Success:    success,
		DurationMs: durationMs,
		Timestamp:  time.Now().UnixMilli(),
		ErrorClass: normalizeErrorClass(success, errorClass),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	event.Sequence = s.sequence

	index := (s.start + s.count) % len(s.events)
	if s.count == len(s.events) {
		index = s.start
		s.start = (s.start + 1) % len(s.events)
	} else {
		s.count++
	}
	s.events[index] = event
}

func (s *Store) SnapshotSince(since uint64) Snapshot {
	if s == nil {
		return Snapshot{Events: []Event{}}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.count == 0 || since >= s.sequence {
		return Snapshot{Instance: s.instance, Sequence: s.sequence, Events: []Event{}}
	}
	oldestSequence := s.sequence - uint64(s.count) + 1
	startSequence := oldestSequence
	if since >= oldestSequence {
		startSequence = since + 1
	}
	offset := int(startSequence - oldestSequence)
	events := make([]Event, 0, s.count-offset)
	for i := offset; i < s.count; i++ {
		event := s.events[(s.start+i)%len(s.events)]
		events = append(events, event)
	}
	return Snapshot{Instance: s.instance, Sequence: s.sequence, Events: events}
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
