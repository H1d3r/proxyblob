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
	return &BaseHandler{
		conn:   conn,
		Ctx:    ctx,
		Cancel: cancel,
	}
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

		packet := Decode(data)
		if packet == nil {
			continue
		}

		errCode := h.handlePacket(packet)
		if errCode != ErrNone {
			if h.Ctx.Err() != nil {
				continue
			}
			// Async close dispatch to avoid blocking ReceiveLoop on aznet writes
			go h.SendClose(packet.ConnectionID, errCode)
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

// sendPacket is the internal method that encodes and sends all packet types.
// It handles error checking and context checking.
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

	_, err := h.conn.Write(encoded)
	if err != nil {
		if err == io.EOF {
			return ErrTransportClosed
		}
		return ErrTransportError
	}

	return ErrNone
}

func (h *BaseHandler) CloseAllConnections() {
	h.Connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Close()
		h.Connections.Delete(key)
		return true
	})
}
