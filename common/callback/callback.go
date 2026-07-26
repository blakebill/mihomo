package callback

import (
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type firstWriteCallBackConn struct {
	C.Conn
	callback func(error, time.Duration)
	state    atomic.Uint32
}

func (c *firstWriteCallBackConn) Write(b []byte) (n int, err error) {
	first, started := c.beginWrite()
	if first {
		defer func() {
			c.finishWrite(err, started)
		}()
	}
	return c.Conn.Write(b)
}

func (c *firstWriteCallBackConn) WriteBuffer(buffer *buf.Buffer) (err error) {
	first, started := c.beginWrite()
	if first {
		defer func() {
			c.finishWrite(err, started)
		}()
	}
	return c.Conn.WriteBuffer(buffer)
}

func (c *firstWriteCallBackConn) beginWrite() (bool, time.Time) {
	if !c.state.CompareAndSwap(0, 1) {
		return false, time.Time{}
	}
	return true, time.Now()
}

func (c *firstWriteCallBackConn) finishWrite(err error, started time.Time) {
	c.state.Store(2)
	c.callback(err, time.Since(started))
}

func (c *firstWriteCallBackConn) Upstream() any {
	return c.Conn
}

func (c *firstWriteCallBackConn) WriterReplaceable() bool {
	return c.state.Load() == 2
}

func (c *firstWriteCallBackConn) ReaderReplaceable() bool {
	return true
}

var _ N.ExtendedConn = (*firstWriteCallBackConn)(nil)

func NewFirstWriteCallBackConn(c C.Conn, callback func(error)) C.Conn {
	return NewFirstWriteLatencyCallBackConn(c, func(err error, _ time.Duration) {
		callback(err)
	})
}

func NewFirstWriteLatencyCallBackConn(c C.Conn, callback func(error, time.Duration)) C.Conn {
	return &firstWriteCallBackConn{
		Conn:     c,
		callback: callback,
	}
}
