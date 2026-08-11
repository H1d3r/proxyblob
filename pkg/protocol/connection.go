package protocol

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Connection manages a proxy connection between client and target.
// It is safe for concurrent use by multiple goroutines.
//
// Connection state is determined by:
//   - ProtocolConn() == nil: connection is pending acknowledgment
//   - ProtocolConn() != nil && Closed not signaled: connection is active
//   - Closed signaled: connection is terminated
type Connection struct {
	// ID uniquely identifies the connection
	ID uuid.UUID

	// Conn holds the network connection (could be real or virtual ProtocolConn)
	Conn net.Conn

	// protoConn is the virtual connection for protocol-based data transfer. It is
	// published from the receive goroutine (via SetProtocolConn on acknowledgment)
	// and read from the connection-handling goroutine, so it is held behind an
	// atomic pointer; access it through ProtocolConn()/SetProtocolConn.
	protoConn atomic.Pointer[ProtocolConn]

	// established is closed exactly once when protoConn is first set, so a waiter
	// can block on acknowledgment via a select instead of polling.
	established     chan struct{}
	establishedOnce sync.Once

	// Closed signals connection termination
	Closed chan struct{}

	// deliverCh is the dispatch buffer between ReceiveLoop and the delivery goroutine.
	// ReceiveLoop writes here via Deliver, which blocks until the payload is
	// accepted (or the connection closes / the handler stops) rather than
	// dropping it; the delivery goroutine drains to readBuffer.
	deliverCh chan []byte

	// stop is the handler-lifetime channel (BaseHandler.Ctx.Done()). It is the
	// escape hatch that releases a Deliver blocked on deliverCh when the
	// handler shuts down without closing this connection individually.
	// Always non-nil; see NewConnection.
	stop <-chan struct{}

	// closeOnce guards Close() against double-close panics
	closeOnce sync.Once

	// CreatedAt records connection creation time
	CreatedAt time.Time

	// LastActivity tracks most recent data transfer
	LastActivity time.Time
}

// neverStop stands in for a nil stop channel. A receive on a nil channel blocks
// forever, which would silently defeat Deliver's escape hatch; substituting an
// explicit, never-closed channel keeps the behavior obvious rather than
// accidental (and keeps the select arm well-defined).
var neverStop = make(chan struct{})

// NewConnection creates a connection with the specified ID. stop is the
// handler-lifetime channel (h.Ctx.Done()); it releases a blocked Deliver when
// the handler shuts down. It must not be nil.
func NewConnection(id uuid.UUID, stop <-chan struct{}) *Connection {
	if stop == nil {
		// Defensive: preserve the "must not be nil" contract without panicking.
		stop = neverStop
	}
	return &Connection{
		ID:           id,
		Closed:       make(chan struct{}),
		established:  make(chan struct{}),
		deliverCh:    make(chan []byte, 1024),
		stop:         stop,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}
}

// ProtocolConn returns the virtual connection, or nil if the connection has not
// yet been acknowledged.
func (c *Connection) ProtocolConn() *ProtocolConn {
	return c.protoConn.Load()
}

// SetProtocolConn publishes the virtual connection and signals establishment.
// The first call unblocks any waiter on Established(); later calls only update
// the pointer (the signal fires once).
func (c *Connection) SetProtocolConn(pc *ProtocolConn) {
	c.protoConn.Store(pc)
	c.establishedOnce.Do(func() { close(c.established) })
}

// Established returns a channel closed once the connection has been
// acknowledged and its virtual connection published.
func (c *Connection) Established() <-chan struct{} {
	return c.established
}

// Close terminates the connection and its resources.
// Safe to call multiple times (guarded by closeOnce). Returns ErrNone.
func (c *Connection) Close() byte {
	c.closeOnce.Do(func() {
		close(c.Closed)
		if pc := c.ProtocolConn(); pc != nil {
			pc.Shutdown()
		}
		if c.Conn != nil {
			c.Conn.Close()
		}
	})
	return ErrNone
}

// Deliver hands data to the connection's delivery goroutine, blocking until the
// payload is accepted. It returns false only when the connection is closed or
// the handler is stopping - never because a buffer was full. Consumer
// back-pressure therefore shows up as a slow Deliver, not as silent data loss.
func (c *Connection) Deliver(data []byte) bool {
	// Pre-check: if Closed is already closed, refuse deterministically. Without
	// this, a random select could enqueue into deliverCh while it still has
	// room even though the StartDelivery goroutine has already returned via its
	// own <-c.Closed case: nothing would ever read the payload, yet Deliver
	// would report success.
	select {
	case <-c.Closed:
		return false
	default:
	}

	select {
	case c.deliverCh <- data:
		return true
	case <-c.Closed:
		return false
	// The stop arm is required, not defensive. A single receive goroutine feeds
	// every connection on the transport, so an unconditional block here would
	// head-of-line-block the whole multiplexed stream - new-connection, ack and
	// close records included - behind one stalled consumer. Waiting on Closed
	// alone is not sufficient: the writer's error path cancels the handler
	// context without closing individual connections, which would wedge Deliver
	// forever.
	case <-c.stop:
		return false
	}
}

// StartDelivery launches a goroutine that drains deliverCh into the virtual
// connection. Must be called after SetProtocolConn, whose store happens-before
// this load.
func (c *Connection) StartDelivery() {
	pc := c.ProtocolConn()
	go func() {
		for {
			select {
			case data, ok := <-c.deliverCh:
				if !ok {
					return
				}
				pc.DeliverData(data)
			case <-c.Closed:
				return
			}
		}
	}()
}
