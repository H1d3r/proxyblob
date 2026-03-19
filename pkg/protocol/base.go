package protocol

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// PacketHandler processes protocol packets and manages connection lifecycle.
// Implementations must be safe for concurrent use by multiple goroutines.
type PacketHandler interface {
	// Start begins packet processing and listens on the specified address (listen only on proxy side)
	Start(string)

	// Stop gracefully terminates all connections and processing
	Stop()

	// ReceiveLoop processes incoming packets until stopped
	ReceiveLoop()

	// OnNew handles connection establishment request
	OnNew(uuid.UUID, []byte) byte

	// OnAck handles connection establishment acknowledgment
	OnAck(uuid.UUID, []byte) byte

	// OnData handles payload transfer for established connection
	OnData(uuid.UUID, []byte) byte

	// OnClose handles connection termination request
	OnClose(uuid.UUID, byte) byte
}

// BaseHandler implements common protocol functionality for proxy and agent.
// It provides connection management, packet routing, and error handling.
type BaseHandler struct {
	// conn handles underlying packet transmission (direct net.Conn)
	conn net.Conn

	// writeCh buffers encoded packets for the writeLoop goroutine
	writeCh chan []byte

	// Connections maps UUIDs to active Connection objects
	Connections sync.Map

	// Ctx controls handler lifecycle
	Ctx context.Context

	// Cancel terminates handler context
	Cancel context.CancelFunc

	// OnReceive is called on every successful packet read (optional)
	OnReceive func()

	// PacketHandler routes packets to specific handlers
	PacketHandler
}

// NewBaseHandler creates a handler with specified context and connection.
// Uses background context if parent context is nil.
func NewBaseHandler(parentCtx context.Context, conn net.Conn) *BaseHandler {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	h := &BaseHandler{
		conn:    conn,
		writeCh: make(chan []byte, 1024),
		Ctx:     ctx,
		Cancel:  cancel,
	}
	go h.writeLoop()
	return h
}

// ReceiveLoop processes incoming packets until EOF or context cancellation.
// Uses exponential backoff on transient errors but never exits silently.
func (h *BaseHandler) ReceiveLoop() {
	consecutiveErrors := 0
	const maxBackoff = 5 * time.Second

	// NOTE: aznet already handles message framing and returns complete messages
	// We don't need stream buffering - each Read() returns ONE complete message

	// Allocate buffer once and reuse it (16MB is the max frame size in aznet)
	buffer := make([]byte, 16*1024*1024)

	for {
		select {
		case <-h.Ctx.Done():
			return
		default:
		}

		// Read directly from connection (aznet handles framing)
		n, err := h.conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				h.Stop()
				return
			}

			if h.Ctx.Err() != nil {
				return
			}

			// Transient error: exponential backoff (100ms, 200ms, 400ms, ... capped at 5s)
			consecutiveErrors++
			backoff := time.Duration(100<<uint(consecutiveErrors-1)) * time.Millisecond
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			// Interruptible sleep
			select {
			case <-h.Ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}

		consecutiveErrors = 0
		if h.OnReceive != nil {
			h.OnReceive()
		}
		data := buffer[:n]

		if len(data) == 0 {
			continue
		}

		// Decode all packets in the message (write coalescing may batch multiple)
		for len(data) >= HeaderSize {
			packet, rest := DecodeNext(data)
			if packet == nil {
				break
			}
			errCode := h.handlePacket(packet)
			if errCode != ErrNone {
				if h.Ctx.Err() != nil {
					break
				}
				// Async close dispatch to avoid blocking ReceiveLoop on aznet writes
				go h.SendClose(packet.ConnectionID, errCode)
			}
			data = rest
		}
	}
}

// handlePacket routes packet to appropriate handler based on command.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) handlePacket(packet *Packet) byte {
	switch packet.Command {
	case CmdNew:
		return h.PacketHandler.OnNew(packet.ConnectionID, packet.Data)
	case CmdAck:
		return h.PacketHandler.OnAck(packet.ConnectionID, packet.Data)
	case CmdData:
		return h.PacketHandler.OnData(packet.ConnectionID, packet.Data)
	case CmdClose:
		return h.PacketHandler.OnClose(packet.ConnectionID, packet.Data[0])
	default:
		return ErrInvalidCommand
	}
}

// SendNewConnection initiates a new connection.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendNewConnection(connectionID uuid.UUID) byte {
	return h.sendPacket(CmdNew, connectionID, nil)
}

// SendConnAck acknowledges connection.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendConnAck(connectionID uuid.UUID) byte {
	return h.sendPacket(CmdAck, connectionID, nil)
}

// SendData sends data.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendData(connectionID uuid.UUID, data []byte) byte {
	// Verify connection exists
	if _, exists := h.Connections.Load(connectionID); !exists {
		return ErrConnectionNotFound
	}

	return h.sendPacket(CmdData, connectionID, data)
}

// SendClose sends a connection termination packet with an error code.
func (h *BaseHandler) SendClose(connectionID uuid.UUID, errCode byte) byte {
	connObj, exists := h.Connections.Load(connectionID)
	if !exists {
		return ErrConnectionNotFound
	}
	conn := connObj.(*Connection)

	conn.Close()
	h.Connections.Delete(connectionID)
	return h.sendPacket(CmdClose, connectionID, []byte{errCode})
}

// sendPacket encodes a packet and submits it to the write coalescing channel.
// The actual aznet write happens asynchronously in writeLoop.
func (h *BaseHandler) sendPacket(cmd byte, connectionID uuid.UUID, data []byte) byte {
	if h.Ctx.Err() != nil {
		return ErrHandlerStopped
	}

	packet := NewPacket(cmd, connectionID, data)
	if packet == nil {
		return ErrInvalidPacket
	}

	encoded := packet.Encode()
	if encoded == nil {
		return ErrInvalidPacket
	}

	select {
	case h.writeCh <- encoded:
		return ErrNone
	case <-h.Ctx.Done():
		return ErrHandlerStopped
	}
}

// writeLoop coalesces queued packets and writes them in batches to aznet.
// Only this goroutine calls conn.Write, eliminating fmu contention.
func (h *BaseHandler) writeLoop() {
	for {
		var buf []byte

		// Wait for the first packet
		select {
		case first := <-h.writeCh:
			buf = append(buf, first...)
		case <-h.Ctx.Done():
			return
		}

		// Drain all queued packets (non-blocking)
		for {
			select {
			case more := <-h.writeCh:
				buf = append(buf, more...)
			default:
				goto flush
			}
		}

	flush:
		if _, err := h.conn.Write(buf); err != nil {
			h.Cancel()
			return
		}
	}
}

func (h *BaseHandler) CloseAllConnections() {
	h.Connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Close()
		h.Connections.Delete(key)
		return true
	})
}
