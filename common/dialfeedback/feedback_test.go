package dialfeedback

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
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
