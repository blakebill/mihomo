package outboundgroup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup/smartengine"
	"github.com/metacubex/mihomo/common/dialfeedback"
	C "github.com/metacubex/mihomo/constant"
)

type mockProxy struct {
	*outbound.Base
	fail  bool
	delay time.Duration
	calls *atomic.Int32
}

func (m *mockProxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if m.calls != nil {
		m.calls.Add(1)
	}
	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.delay):
		}
	}
	if m.fail {
		return nil, errors.New("dial failed")
	}
	c1, c2 := net.Pipe()
	go c2.Close()
	return outbound.NewConn(c1, m), nil
}

type handshakeMockConn struct {
	C.Conn
}

func (*handshakeMockConn) NeedHandshake() bool {
	return true
}

type handshakeMockProxy struct {
	*outbound.Base
	dialDelay time.Duration
}

func (m *handshakeMockProxy) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	if m.dialDelay > 0 {
		time.Sleep(m.dialDelay)
	}
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		buffer := make([]byte, 4)
		if _, err := server.Read(buffer); err == nil {
			_, _ = server.Write([]byte("ok"))
		}
	}()
	return &handshakeMockConn{Conn: outbound.NewConn(client, m)}, nil
}

func newSmartWithProxies(t *testing.T, proxies ...C.Proxy) *Smart {
	t.Helper()
	fb := adapter.NewProxy(outbound.NewReject())
	s, err := NewSmart(GroupCommonOption{
		Name: "smart-test",
		Type: "smart",
	}, SmartOption{}, fb, nil)
	if err != nil {
		t.Fatalf("NewSmart: %v", err)
	}
	// Inject cached proxies (empty providers → versions always match).
	s.providerProxies = proxies
	return s
}

func TestSmartDialContextRetriesOnFailure(t *testing.T) {
	t.Parallel()
	var badCalls, goodCalls atomic.Int32
	bad := adapter.NewProxy(&mockProxy{
		Base:  outbound.NewBase(outbound.BaseOption{Name: "bad", Type: C.Direct}),
		fail:  true,
		calls: &badCalls,
	})
	good := adapter.NewProxy(&mockProxy{
		Base:  outbound.NewBase(outbound.BaseOption{Name: "good", Type: C.Direct}),
		calls: &goodCalls,
	})
	s := newSmartWithProxies(t, bad, good)
	s.ensureEngine([]C.Proxy{bad, good})
	// Pin sticky so the failing member is attempted first.
	s.eng.RememberHost("example.com", "bad")

	meta := &C.Metadata{Host: "example.com", DstPort: 443}
	conn, err := s.DialContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	_ = conn.Close()

	if badCalls.Load() < 1 {
		t.Fatalf("expected bad attempt, calls=%d", badCalls.Load())
	}
	if goodCalls.Load() < 1 {
		t.Fatalf("expected good used, calls=%d", goodCalls.Load())
	}
	if s.Now() != "good" {
		t.Fatalf("selected=%q want good", s.Now())
	}
}

func TestSmartDialContextSoftFailRetries(t *testing.T) {
	t.Parallel()
	var slowCalls, fastCalls atomic.Int32
	slow := adapter.NewProxy(&mockProxy{
		Base:  outbound.NewBase(outbound.BaseOption{Name: "slow", Type: C.Direct}),
		delay: 300 * time.Millisecond,
		calls: &slowCalls,
	})
	fast := adapter.NewProxy(&mockProxy{
		Base:  outbound.NewBase(outbound.BaseOption{Name: "fast", Type: C.Direct}),
		calls: &fastCalls,
	})
	s := newSmartWithProxies(t, slow, fast)
	// Seed low EWMA + sticky so first attempt is slow and trips soft-fail.
	s.ensureEngine([]C.Proxy{slow, fast})
	s.eng.Record("slow", engine.OutcomeSuccess, 50)
	s.eng.Record("slow", engine.OutcomeSuccess, 50)
	s.eng.RememberHost("soft.example", "slow")

	meta := &C.Metadata{Host: "soft.example", DstPort: 443}
	conn, err := s.DialContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	_ = conn.Close()
	if slowCalls.Load() < 1 || fastCalls.Load() < 1 {
		t.Fatalf("expected soft-fail retry slow=%d fast=%d", slowCalls.Load(), fastCalls.Load())
	}
	if s.Now() != "fast" {
		t.Fatalf("selected=%q want fast", s.Now())
	}
}

func TestSmartDialContextFallsBackWhenAllFail(t *testing.T) {
	t.Parallel()
	bad1 := adapter.NewProxy(&mockProxy{
		Base: outbound.NewBase(outbound.BaseOption{Name: "a", Type: C.Direct}),
		fail: true,
	})
	bad2 := adapter.NewProxy(&mockProxy{
		Base: outbound.NewBase(outbound.BaseOption{Name: "b", Type: C.Direct}),
		fail: true,
	})
	s := newSmartWithProxies(t, bad1, bad2)
	meta := &C.Metadata{Host: "example.com", DstPort: 443}
	_, err := s.DialContext(context.Background(), meta)
	// Reject emptyFallback also fails — either last dial error or reject is fine.
	if err == nil {
		t.Fatal("expected error when all members fail")
	}
}

func TestSmartDialContextRecordsPrivacySafeFeedback(t *testing.T) {
	proxy := adapter.NewProxy(&mockProxy{
		Base: outbound.NewBase(outbound.BaseOption{Name: "feedback-node", Type: C.Direct}),
	})
	s := newSmartWithProxies(t, proxy)
	startSequence := dialfeedback.Default.SnapshotSince(0).Sequence
	meta := &C.Metadata{Host: "private.example", DstPort: 443}
	conn, err := s.DialContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	_ = conn.Close()

	events := dialfeedback.Default.SnapshotSince(startSequence).Events
	if len(events) != 1 {
		t.Fatalf("events=%d want 1: %#v", len(events), events)
	}
	event := events[0]
	if event.Group != "smart-test" || event.Outbound != "feedback-node" || event.Signal != "tcp" ||
		!event.Success || event.Network != "tcp" {
		t.Fatalf("unexpected feedback: %#v", event)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal feedback: %v", err)
	}
	if bytes.Contains(payload, []byte("private.example")) {
		t.Fatalf("feedback leaked target: %s", payload)
	}
}

func TestSmartDialContextRecordsOptInStageSignals(t *testing.T) {
	proxy := adapter.NewProxy(&handshakeMockProxy{
		Base:      outbound.NewBase(outbound.BaseOption{Name: "stage-node", Type: C.Direct}),
		dialDelay: 20 * time.Millisecond,
	})
	s := newSmartWithProxies(t, proxy)
	startSequence := dialfeedback.Default.SnapshotSince(0).Sequence
	conn, err := s.DialContext(context.Background(), &C.Metadata{Host: "private.example", DstPort: 443})
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buffer := make([]byte, 2)
	if _, err = conn.Read(buffer); err != nil {
		t.Fatalf("read: %v", err)
	}

	all := dialfeedback.Default.SnapshotDetailedSince(startSequence)
	if len(all.Events) != 3 {
		t.Fatalf("stage events=%d want 3: %#v", len(all.Events), all.Events)
	}
	signals := []string{all.Events[0].Signal, all.Events[1].Signal, all.Events[2].Signal}
	if signals[0] != "tcp" || signals[1] != "handshake" || signals[2] != "first-byte" {
		t.Fatalf("signals=%v", signals)
	}
	for _, event := range all.Events {
		if !event.Success || event.Network != "tcp" || event.Outbound != "stage-node" {
			t.Fatalf("unexpected stage event: %#v", event)
		}
	}

	legacy := dialfeedback.Default.SnapshotLegacySince(startSequence)
	if len(legacy.Events) != 1 || legacy.Events[0].Signal != "handshake" {
		t.Fatalf("legacy events changed: %#v", legacy.Events)
	}
	if legacy.Sequence != all.Sequence {
		t.Fatalf("legacy sequence=%d all=%d", legacy.Sequence, all.Sequence)
	}
	if all.Events[0].DurationMs < 15 || legacy.Events[0].DurationMs <= all.Events[1].DurationMs {
		t.Fatalf("stage durations detailed=%#v legacy=%#v", all.Events, legacy.Events)
	}
}

func TestFirstByteObserveConn(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	adapter := &mockProxy{Base: outbound.NewBase(outbound.BaseOption{Name: "first-byte", Type: C.Direct})}
	observed := make(chan error, 1)
	conn := newFirstByteObserveConn(outbound.NewConn(client, adapter), func(err error, _ time.Duration) {
		observed <- err
	})
	go func() {
		buffer := make([]byte, 4)
		_, _ = server.Read(buffer)
		_, _ = server.Write([]byte("ok"))
	}()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buffer := make([]byte, 2)
	if _, err := conn.Read(buffer); err != nil {
		t.Fatalf("read: %v", err)
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Fatalf("first byte marked failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first-byte observation timed out")
	}
}

func TestFirstByteObserveConnRecordsFailure(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	adapter := &mockProxy{Base: outbound.NewBase(outbound.BaseOption{Name: "first-byte-failure", Type: C.Direct})}
	observed := make(chan error, 1)
	conn := newFirstByteObserveConn(outbound.NewConn(client, adapter), func(err error, _ time.Duration) {
		observed <- err
	})
	go func() {
		buffer := make([]byte, 4)
		_, _ = server.Read(buffer)
		_ = server.Close()
	}()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buffer := make([]byte, 2)
	if _, err := conn.Read(buffer); err == nil {
		t.Fatal("read unexpectedly succeeded")
	}
	select {
	case err := <-observed:
		if err == nil {
			t.Fatal("first-byte failure marked successful")
		}
	case <-time.After(time.Second):
		t.Fatal("first-byte failure observation timed out")
	}
}
