package protocol

import (
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Connection manages a proxy connection between client and target.
// It is safe for concurrent use by multiple goroutines.
//
// Connection state is determined by:
//   - ProtocolConn == nil: connection is pending acknowledgment
//   - ProtocolConn != nil && Closed not signaled: connection is active
//   - Closed signaled: connection is terminated
type Connection struct {
	// ID uniquely identifies the connection
	ID uuid.UUID

	// Conn holds the network connection (could be real or virtual ProtocolConn)
	Conn net.Conn

	// ProtocolConn is the virtual connection for protocol-based data transfer
	ProtocolConn *ProtocolConn

	// Closed signals connection termination
	Closed chan struct{}

	// deliverCh is the dispatch buffer between ReceiveLoop and the delivery goroutine.
	// ReceiveLoop writes here non-blocking; the delivery goroutine drains to readBuffer.
	deliverCh chan []byte

	// closeOnce guards Close() against double-close panics
	closeOnce sync.Once

	// CreatedAt records connection creation time
	CreatedAt time.Time

	// LastActivity tracks most recent data transfer
	LastActivity time.Time
}

// NewConnection creates a connection with specified ID.
// Initializes channels and sets initial timestamps.
func NewConnection(id uuid.UUID) *Connection {
	return &Connection{
		ID:           id,
		Closed:       make(chan struct{}),
		deliverCh:    make(chan []byte, 1024),
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}
}

// Close terminates the connection and its resources.
// Safe to call multiple times (guarded by closeOnce). Returns ErrNone.
func (c *Connection) Close() byte {
	c.closeOnce.Do(func() {
		close(c.Closed)
		if c.ProtocolConn != nil {
			c.ProtocolConn.Shutdown()
		}
		if c.Conn != nil {
			c.Conn.Close()
		}
	})
	return ErrNone
}

// Deliver sends data to the connection's delivery goroutine without blocking.
// Returns true if data was accepted, false if the buffer is full or connection is closed.
func (c *Connection) Deliver(data []byte) bool {
	select {
	case c.deliverCh <- data:
		return true
	case <-c.Closed:
		return false
	default:
		return false // dispatch buffer full
	}
}

// StartDelivery launches a goroutine that drains deliverCh into ProtocolConn.DeliverData.
// Must be called after ProtocolConn is set.
func (c *Connection) StartDelivery() {
	go func() {
		for {
			select {
			case data, ok := <-c.deliverCh:
				if !ok {
					return
				}
				c.ProtocolConn.DeliverData(data)
			case <-c.Closed:
				return
			}
		}
	}()
}
