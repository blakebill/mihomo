package dialfeedback

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStoreIsBoundedAndIncremental(t *testing.T) {
	store := NewStore(2)
	otherStore := NewStore(2)
	store.Add("smart", "a", "tcp", true, 12*time.Millisecond, "")
	store.Add("smart", "b", "tcp", false, 23*time.Millisecond, "network")
	store.Add("smart", "c", "udp", false, 34*time.Millisecond, "timeout")

	snapshot := store.SnapshotSince(0)
	if snapshot.Sequence != 3 {
		t.Fatalf("sequence=%d want 3", snapshot.Sequence)
	}
	if len(snapshot.Instance) != 32 || snapshot.Instance == otherStore.SnapshotSince(0).Instance {
		t.Fatalf("invalid or reused instance ID: %q", snapshot.Instance)
	}
	if _, err := hex.DecodeString(snapshot.Instance); err != nil {
		t.Fatalf("instance ID is not valid hex: %q", snapshot.Instance)
	}
	if snapshot.Instance != strings.ToLower(snapshot.Instance) {
		t.Fatalf("instance ID is not lowercase: %q", snapshot.Instance)
	}
	if len(snapshot.Events) != 2 || snapshot.Events[0].Outbound != "b" || snapshot.Events[1].Outbound != "c" {
		t.Fatalf("unexpected bounded events: %#v", snapshot.Events)
	}
	if snapshot.Events[0].Signal != "tcp" || snapshot.Events[1].Signal != "udp" {
		t.Fatalf("default signals were not inferred: %#v", snapshot.Events)
	}
	if cap(snapshot.Events) != len(snapshot.Events) {
		t.Fatalf("bounded snapshot capacity=%d events=%d", cap(snapshot.Events), len(snapshot.Events))
	}

	incremental := store.SnapshotSince(2)
	if len(incremental.Events) != 1 || incremental.Events[0].Sequence != 3 {
		t.Fatalf("unexpected incremental events: %#v", incremental.Events)
	}
	if incremental.Instance != snapshot.Instance || cap(incremental.Events) != len(incremental.Events) {
		t.Fatalf("incremental snapshot was not stable/exact: %#v", incremental)
	}
	current := store.SnapshotSince(snapshot.Sequence)
	if current.Instance != snapshot.Instance || current.Events == nil || len(current.Events) != 0 || cap(current.Events) != 0 {
		t.Fatalf("current cursor returned unstable/retained data: %#v", current)
	}
}

func TestStoreStageSignalsAndLegacyFilter(t *testing.T) {
	store := NewStore(8)
	store.AddSupplementalSignal("smart", "node", "tcp", "tcp", true, time.Millisecond, "")
	store.AddLegacySignalDurations(
		"smart",
		"node",
		"tcp",
		"handshake",
		true,
		2*time.Millisecond,
		22*time.Millisecond,
		"",
	)
	store.AddSupplementalSignal("smart", "node", "tcp", "first-byte", false, 3*time.Millisecond, "network")
	store.AddSupplementalSignal("smart", "node", "udp", "private.example", false, 4*time.Millisecond, "secret error")

	all := store.SnapshotDetailedSince(0)
	if len(all.Events) != 4 || cap(all.Events) != 4 {
		t.Fatalf("all signals=%#v", all.Events)
	}
	wantSignals := []string{"tcp", "handshake", "first-byte", "udp"}
	for index, event := range all.Events {
		if event.Signal != wantSignals[index] {
			t.Fatalf("signal[%d]=%q want %q", index, event.Signal, wantSignals[index])
		}
	}
	if all.Events[3].ErrorClass != "unknown" {
		t.Fatalf("raw error class escaped normalization: %#v", all.Events[3])
	}

	legacy := store.SnapshotLegacySince(0)
	if legacy.Sequence != all.Sequence || len(legacy.Events) != 1 || cap(legacy.Events) != 1 {
		t.Fatalf("legacy snapshot=%#v", legacy)
	}
	if legacy.Events[0].Signal != "handshake" {
		t.Fatalf("legacy signal=%#v", legacy.Events[0])
	}
	if all.Events[1].DurationMs != 2 || legacy.Events[0].DurationMs != 22 {
		t.Fatalf("duration projection detailed=%#v legacy=%#v", all.Events[1], legacy.Events[0])
	}
	payload, err := json.Marshal(all)
	if err != nil {
		t.Fatalf("marshal signals: %v", err)
	}
	if strings.Contains(string(payload), "private.example") || strings.Contains(string(payload), "secret error") ||
		strings.Contains(string(payload), "legacy") {
		t.Fatalf("signal snapshot leaked private/internal data: %s", payload)
	}

	hiddenOnly := NewStore(2)
	hiddenOnly.AddSupplementalSignal("smart", "node", "tcp", "first-byte", true, time.Millisecond, "")
	hiddenSnapshot := hiddenOnly.SnapshotLegacySince(0)
	if hiddenSnapshot.Sequence != 1 || hiddenSnapshot.Events == nil || len(hiddenSnapshot.Events) != 0 {
		t.Fatalf("hidden signal did not advance legacy cursor safely: %#v", hiddenSnapshot)
	}
}

func TestStoreLegacyRetentionIsIndependent(t *testing.T) {
	store := NewStore(4)
	names := []string{"a", "b", "c", "d", "e", "f"}
	for _, name := range names {
		store.AddSupplementalSignal("smart", name, "tcp", "tcp", true, time.Millisecond, "")
		store.AddLegacySignal("smart", name, "tcp", "handshake", true, 2*time.Millisecond, "")
		store.AddSupplementalSignal("smart", name, "tcp", "first-byte", true, 3*time.Millisecond, "")
	}

	detailed := store.SnapshotDetailedSince(0)
	legacy := store.SnapshotLegacySince(0)
	if detailed.Sequence != 18 || legacy.Sequence != detailed.Sequence ||
		len(detailed.Events) != 4 || len(legacy.Events) != 4 {
		t.Fatalf("retention detailed=%#v legacy=%#v", detailed, legacy)
	}
	wantLegacy := []string{"c", "d", "e", "f"}
	for index, event := range legacy.Events {
		if event.Outbound != wantLegacy[index] || event.Signal != "handshake" {
			t.Fatalf("legacy[%d]=%#v want outbound %q", index, event, wantLegacy[index])
		}
	}
	incremental := store.SnapshotLegacySince(11)
	if len(incremental.Events) != 2 || incremental.Events[0].Outbound != "e" ||
		incremental.Events[1].Outbound != "f" || cap(incremental.Events) != 2 {
		t.Fatalf("legacy incremental=%#v", incremental)
	}
}

func TestStoreConcurrentSignals(t *testing.T) {
	store := NewStore(32)
	const writers = 128
	var waitGroup sync.WaitGroup
	waitGroup.Add(writers + 1)
	for index := 0; index < writers; index++ {
		go func(index int) {
			defer waitGroup.Done()
			signal := "tcp"
			network := "tcp"
			if index%2 != 0 {
				signal = "udp"
				network = "udp"
			}
			if index%3 == 0 {
				store.AddSupplementalSignal("smart", "node", network, signal, true, time.Millisecond, "")
			} else {
				store.AddLegacySignal("smart", "node", network, signal, true, time.Millisecond, "")
			}
		}(index)
	}
	go func() {
		defer waitGroup.Done()
		for index := 0; index < writers; index++ {
			_ = store.SnapshotDetailedSince(uint64(index))
			_ = store.SnapshotLegacySince(uint64(index))
		}
	}()
	waitGroup.Wait()

	snapshot := store.SnapshotDetailedSince(0)
	if snapshot.Sequence != writers || len(snapshot.Events) != 32 {
		t.Fatalf("concurrent snapshot sequence=%d events=%d", snapshot.Sequence, len(snapshot.Events))
	}
	legacy := store.SnapshotLegacySince(0)
	if legacy.Sequence != writers || len(legacy.Events) != 32 {
		t.Fatalf("concurrent legacy sequence=%d events=%d", legacy.Sequence, len(legacy.Events))
	}
	compatible := store.SnapshotSince(0)
	if len(compatible.Events) != len(legacy.Events) {
		t.Fatalf("SnapshotSince compatibility changed: got %d events want %d", len(compatible.Events), len(legacy.Events))
	}
}

func TestStoreNormalizesEventData(t *testing.T) {
	store := NewStore(2)
	store.Add("smart", "node", "tcp", true, -time.Second, "must-not-leak")
	store.Add("smart", "node", "tcp", false, time.Second, "private.example:443")
	events := store.SnapshotSince(0).Events
	if events[0].DurationMs != 0 || events[0].ErrorClass != "" || events[0].Timestamp <= 0 {
		t.Fatalf("unexpected normalized success: %#v", events[0])
	}
	if events[1].ErrorClass != "unknown" {
		t.Fatalf("unsafe failure class was not normalized: %#v", events[1])
	}
	store.Add("smart", "node", "tcp", true, time.Microsecond, "")
	if got := store.SnapshotSince(2).Events[0].DurationMs; got != 1 {
		t.Fatalf("sub-millisecond duration=%d want 1", got)
	}
}

func TestErrorClass(t *testing.T) {
	if got := ErrorClass(context.Canceled); got != "canceled" {
		t.Fatalf("canceled class=%q", got)
	}
	if got := ErrorClass(context.DeadlineExceeded); got != "timeout" {
		t.Fatalf("deadline class=%q", got)
	}
	if got := ErrorClass(errors.New("contains private destination 192.0.2.1")); got != "network" {
		t.Fatalf("generic class=%q", got)
	}
}
