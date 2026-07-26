package callback

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type latencyTestNetConn struct {
	writeDelay time.Duration
}

func (c *latencyTestNetConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *latencyTestNetConn) Close() error                     { return nil }
func (c *latencyTestNetConn) LocalAddr() net.Addr              { return nil }
func (c *latencyTestNetConn) RemoteAddr() net.Addr             { return nil }
func (c *latencyTestNetConn) SetDeadline(time.Time) error      { return nil }
func (c *latencyTestNetConn) SetReadDeadline(time.Time) error  { return nil }
func (c *latencyTestNetConn) SetWriteDeadline(time.Time) error { return nil }
func (c *latencyTestNetConn) Write(payload []byte) (int, error) {
	time.Sleep(c.writeDelay)
	return len(payload), nil
}

type latencyTestConn struct {
	N.ExtendedConn
}

func (*latencyTestConn) Chains() C.Chain               { return nil }
func (*latencyTestConn) ProviderChains() C.Chain       { return nil }
func (*latencyTestConn) AppendToChains(C.ProxyAdapter) {}
func (*latencyTestConn) RemoteDestination() string     { return "" }

func TestFirstWriteLatencyExcludesCallerWait(t *testing.T) {
	base := &latencyTestConn{
		ExtendedConn: N.NewExtendedConn(&latencyTestNetConn{writeDelay: 5 * time.Millisecond}),
	}
	durations := make(chan time.Duration, 2)
	conn := NewFirstWriteLatencyCallBackConn(base, func(err error, duration time.Duration) {
		if err != nil {
			t.Errorf("callback error: %v", err)
		}
		durations <- duration
	})

	// Waiting before the caller's first Write is not handshake latency.
	time.Sleep(75 * time.Millisecond)
	if _, err := conn.Write([]byte("first")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, err := conn.Write([]byte("second")); err != nil {
		t.Fatalf("second write: %v", err)
	}

	duration := <-durations
	if duration < 4*time.Millisecond || duration >= 50*time.Millisecond {
		t.Fatalf("first-write duration=%s", duration)
	}
	select {
	case duplicate := <-durations:
		t.Fatalf("callback ran twice, second duration=%s", duplicate)
	default:
	}
}

func TestFirstWriteLatencyConcurrentWriteCallsBackOnce(t *testing.T) {
	base := &latencyTestConn{
		ExtendedConn: N.NewExtendedConn(&latencyTestNetConn{writeDelay: 5 * time.Millisecond}),
	}
	var callbackCount atomic.Int32
	conn := NewFirstWriteLatencyCallBackConn(base, func(err error, _ time.Duration) {
		if err != nil {
			t.Errorf("callback error: %v", err)
		}
		callbackCount.Add(1)
	})

	const writers = 32
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer waitGroup.Done()
			<-start
			if _, err := conn.Write([]byte("payload")); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	close(start)
	waitGroup.Wait()

	if count := callbackCount.Load(); count != 1 {
		t.Fatalf("callback count=%d, want 1", count)
	}
	replaceableConn, ok := conn.(interface{ WriterReplaceable() bool })
	if !ok {
		t.Fatal("connection does not expose WriterReplaceable")
	}
	if !replaceableConn.WriterReplaceable() {
		t.Fatal("writer should be replaceable after the first write completes")
	}
}
