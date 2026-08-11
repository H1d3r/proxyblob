package protocol

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ProtocolConn implements net.Conn by wrapping the protocol layer.
// This allows using standard io.Copy for data transfer, just like armon/go-socks5.
type ProtocolConn struct {
	id           uuid.UUID
	handler      *BaseHandler
	readBuffer   chan []byte // internal buffer for received data
	readData     []byte      // partial read buffer
	readOffset   int
	closed       chan struct{}
	closeOnce    sync.Once // guards SendClose (protocol-level close)
	shutdownOnce sync.Once // guards close(closed) (local shutdown)
	ctx          context.Context
}

// NewProtocolConn creates a virtual connection that uses the protocol layer.
func NewProtocolConn(ctx context.Context, id uuid.UUID, handler *BaseHandler) *ProtocolConn {
	return &ProtocolConn{
		id:         id,
		handler:    handler,
		readBuffer: make(chan []byte, 1024), // Buffered channel for received data
		closed:     make(chan struct{}),
		ctx:        ctx,
	}
}

// Read implements net.Conn.Read - reads data received via protocol.
func (c *ProtocolConn) Read(b []byte) (n int, err error) {
	// If we have buffered data, return it first
	if c.readOffset < len(c.readData) {
		n = copy(b, c.readData[c.readOffset:])
		c.readOffset += n
		if c.readOffset >= len(c.readData) {
			c.readData = nil
			c.readOffset = 0
		}
		return n, nil
	}

	// Wait for new data from protocol layer.
	//
	// Buffered data must always win over the termination signals. A single
	// three-way select would not guarantee that: Go picks uniformly at random
	// among ready cases, so once closed (or ctx) fires, data still sitting in
	// readBuffer would be discarded roughly half the time per call, and always
	// eventually. That is the normal end-of-response path (the peer's close
	// record arrives on the same shared receive goroutine microseconds after
	// its last data record), and it silently truncates the response body.
	//
	// So: try a non-blocking receive first, and only fall back to honoring
	// closed/ctx when readBuffer is genuinely empty.
	var (
		data []byte
		ok   bool
	)
	select {
	case data, ok = <-c.readBuffer:
	default:
		select {
		case <-c.closed:
			return 0, io.EOF
		case <-c.ctx.Done():
			return 0, io.EOF
		case data, ok = <-c.readBuffer:
		}
	}
	if !ok {
		// readBuffer was closed.
		return 0, io.EOF
	}

	// Copy as much as possible to b
	n = copy(b, data)
	// If there's leftover data, store it
	if n < len(data) {
		c.readData = data
		c.readOffset = n
	}
	return n, nil
}

// Write implements net.Conn.Write - sends data via protocol.
func (c *ProtocolConn) Write(b []byte) (n int, err error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	case <-c.ctx.Done():
		return 0, io.ErrClosedPipe
	default:
	}

	errCode := c.handler.SendData(c.id, b)
	if errCode != ErrNone {
		return 0, io.ErrClosedPipe
	}
	return len(b), nil
}

// Shutdown closes the ProtocolConn's closed channel without sending a CmdClose packet.
// Called by Connection.Close() to unblock delivery goroutines and ProtocolConn.Read().
// Safe to call multiple times.
func (c *ProtocolConn) Shutdown() {
	c.shutdownOnce.Do(func() {
		close(c.closed)
	})
}

// Close implements net.Conn.Close.
// Sends a CmdClose packet and then shuts down locally.
func (c *ProtocolConn) Close() error {
	c.closeOnce.Do(func() {
		c.handler.SendClose(c.id, ErrNone)
	})
	c.Shutdown()
	return nil
}

// LocalAddr implements net.Conn.LocalAddr (returns dummy address).
func (c *ProtocolConn) LocalAddr() net.Addr {
	return &protocolAddr{network: "protocol", address: c.id.String()}
}

// RemoteAddr implements net.Conn.RemoteAddr (returns dummy address).
func (c *ProtocolConn) RemoteAddr() net.Addr {
	return &protocolAddr{network: "protocol", address: "remote"}
}

// SetDeadline implements net.Conn.SetDeadline (not implemented).
func (c *ProtocolConn) SetDeadline(t time.Time) error {
	return nil // Not implemented for protocol connections
}

// SetReadDeadline implements net.Conn.SetReadDeadline (not implemented).
func (c *ProtocolConn) SetReadDeadline(t time.Time) error {
	return nil // Not implemented for protocol connections
}

// SetWriteDeadline implements net.Conn.SetWriteDeadline (not implemented).
func (c *ProtocolConn) SetWriteDeadline(t time.Time) error {
	return nil // Not implemented for protocol connections
}

// DeliverData is called by the protocol handler when data arrives for this connection.
func (c *ProtocolConn) DeliverData(data []byte) {
	// Same reasoning as Read: a two-way select on closed and the send would
	// drop a delivery at random whenever both are ready, even though
	// readBuffer had room for it. Prefer the send whenever it can proceed;
	// Read drains buffered data before reporting EOF, so data accepted after
	// closed is still handed to the consumer.
	select {
	case c.readBuffer <- data:
		return
	default:
	}

	// readBuffer is full: block until there is room, or until the connection
	// is torn down.
	select {
	case <-c.closed:
		return
	case c.readBuffer <- data:
	}
}

// protocolAddr implements net.Addr for protocol connections.
type protocolAddr struct {
	network string
	address string
}

func (a *protocolAddr) Network() string {
	return a.network
}

func (a *protocolAddr) String() string {
	return a.address
}
