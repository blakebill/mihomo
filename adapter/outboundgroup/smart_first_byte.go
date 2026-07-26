package outboundgroup

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

// firstByteObserveConn measures from the first request write to the first read.
// It observes only; replay after a partial write remains deliberately disabled.
type firstByteObserveConn struct {
	C.Conn
	armOnce     sync.Once
	requestAt   time.Time
	armed       atomic.Bool
	observed    atomic.Bool
	onFirstByte func(err error, latency time.Duration)
}

func newFirstByteObserveConn(conn C.Conn, callback func(error, time.Duration)) C.Conn {
	return &firstByteObserveConn{Conn: conn, onFirstByte: callback}
}

func (c *firstByteObserveConn) arm() {
	c.armOnce.Do(func() {
		c.requestAt = time.Now()
		c.armed.Store(true)
	})
}

func (c *firstByteObserveConn) observe(err error) {
	if c.observed.Swap(true) {
		return
	}
	c.arm()
	latency := time.Since(c.requestAt)
	if c.onFirstByte != nil {
		c.onFirstByte(err, latency)
	}
}

func (c *firstByteObserveConn) Write(p []byte) (int, error) {
	c.arm()
	return c.Conn.Write(p)
}

func (c *firstByteObserveConn) WriteBuffer(buffer *buf.Buffer) error {
	c.arm()
	return c.Conn.WriteBuffer(buffer)
}

func (c *firstByteObserveConn) Read(p []byte) (int, error) {
	c.arm()
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.observe(nil)
	} else if err != nil {
		c.observe(err)
	}
	return n, err
}

func (c *firstByteObserveConn) ReadBuffer(buffer *buf.Buffer) error {
	c.arm()
	before := buffer.Len()
	err := c.Conn.ReadBuffer(buffer)
	if buffer.Len() > before {
		c.observe(nil)
	} else if err != nil {
		c.observe(err)
	}
	return err
}

func (c *firstByteObserveConn) Upstream() any {
	return c.Conn
}

func (c *firstByteObserveConn) WriterReplaceable() bool {
	return c.armed.Load()
}

func (c *firstByteObserveConn) ReaderReplaceable() bool {
	return c.observed.Load()
}

var _ N.ExtendedConn = (*firstByteObserveConn)(nil)
