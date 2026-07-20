package outboundgroup

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup/smartengine"
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
