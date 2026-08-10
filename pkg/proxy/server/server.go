// Package proxy implements a SOCKS proxy server.
// It accepts client connections and forwards traffic through transport channels
// to remote agents. The server manages connection lifecycle and bidirectional
// data transfer.
package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"proxyblob/pkg/protocol"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// ProxyServer implements a SOCKS proxy server that forwards traffic transparently.
// It accepts client connections and manages the protocol flow between clients and
// remote agents.
type ProxyServer struct {
	// BaseHandler provides common protocol functionality
	*protocol.BaseHandler

	// Listener accepts incoming TCP connections
	Listener net.Listener
}

// NewProxyServer creates a proxy server instance with the given connection.
// The connection is used for communication with remote agents.
func NewProxyServer(ctx context.Context, conn net.Conn) *ProxyServer {
	server := &ProxyServer{}
	server.BaseHandler = protocol.NewBaseHandler(ctx, conn)
	server.PacketHandler = server
	return server
}

// Start begins listening for client connections on the specified address.
// It launches background goroutines for accepting connections and processing
// protocol messages. If listening fails, the server is stopped.
func (s *ProxyServer) Start(address string) {
	var err error
	s.Listener, err = net.Listen("tcp", address)
	if err != nil {
		log.Error().Err(err).Str("addr", address).Msg("Failed to listen on address")
		s.Stop()
		return
	}

	go s.ReceiveLoop()
	go s.acceptLoop()
}

// Stop gracefully terminates the proxy server by closing all active
// connections, canceling the handler's context, and stopping the listener.
func (s *ProxyServer) Stop() {
	s.CloseAllConnections()
	s.Cancel()
	if s.Listener != nil {
		s.Listener.Close()
	}
}

// OnNew handles new connection requests. The server is the only one initiating
// connections, so this always returns ErrUnexpectedPacket.
func (s *ProxyServer) OnNew(connectionID uuid.UUID, data []byte) byte {
	return protocol.ErrUnexpectedPacket
}

// OnAck processes connection acknowledgments from agents.
// Returns an error code indicating success or failure.
func (s *ProxyServer) OnAck(connectionID uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrConnectionNotFound
	}
	conn := value.(*protocol.Connection)

	// Check if connection already established (ProtocolConn should be nil for new connections)
	if conn.ProtocolConn != nil {
		return protocol.ErrInvalidState
	}

	// Create the virtual protocol connection (marks connection as established)
	conn.ProtocolConn = protocol.NewProtocolConn(s.Ctx, connectionID, s.BaseHandler)
	conn.StartDelivery()
	conn.LastActivity = time.Now()
	return protocol.ErrNone
}

// OnData processes data received from agents and forwards it to the client.
// Returns an error code indicating success or failure.
func (s *ProxyServer) OnData(connectionID uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrConnectionNotFound
	}
	conn := value.(*protocol.Connection)
	conn.LastActivity = time.Now()

	// The connection is only ready to receive data once OnAck has created the
	// virtual protocol connection. Dropping the payload here would be silent
	// data loss, so report the unexpected state instead.
	if conn.ProtocolConn == nil {
		return protocol.ErrInvalidState
	}

	// Deliver blocks until the payload is accepted by the per-connection
	// goroutine, so back-pressure never discards data. A false return means the
	// connection is closed or the handler is shutting down.
	if !conn.Deliver(data) {
		return protocol.ErrConnectionClosed
	}
	return protocol.ErrNone
}

// OnClose handles connection termination from agents. It cleans up the
// connection state.
func (s *ProxyServer) OnClose(connectionID uuid.UUID, errorCode byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrNone // Connection already removed, nothing to do
	}
	conn := value.(*protocol.Connection)
	conn.Close()
	s.Connections.Delete(connectionID)
	return protocol.ErrNone
}

// cleanupConnection closes all connection resources and removes from connection map.
// This helper reduces code duplication in handleConnection.
func (s *ProxyServer) cleanupConnection(connID uuid.UUID, clientConn net.Conn, proxyConn *protocol.Connection) {
	clientConn.Close()
	if proxyConn.ProtocolConn != nil {
		proxyConn.ProtocolConn.Close()
	}
	proxyConn.Close()
	s.Connections.Delete(connID)
}

// acceptLoop accepts incoming TCP connections and spawns goroutines to handle
// each one. It continues until the context is canceled or a non-temporary
// error occurs.
func (s *ProxyServer) acceptLoop() {
	for {
		select {
		case <-s.Ctx.Done():
			return
		default:
			conn, err := s.Listener.Accept()
			if err != nil {
				if s.Ctx.Err() != nil {
					return // Exit quietly on shutdown
				}

				if _, ok := err.(net.Error); ok {
					continue // Retry on temporary network errors
				}
				return
			}

			go s.handleConnection(conn)
		}
	}
}

// handleConnection processes a new client connection by:
//   - Generating a unique connection ID
//   - Initiating connection with remote agent
//   - Setting up bidirectional data forwarding
//   - Managing connection lifecycle and cleanup
func (s *ProxyServer) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// Enable TCP_NODELAY to disable Nagle's algorithm for better TLS performance
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	connID := uuid.New()
	proxyConn := protocol.NewConnection(connID, s.Ctx.Done())
	s.Connections.Store(proxyConn.ID, proxyConn)

	// 1. Initiate connection with the agent
	errCode := s.SendNewConnection(connID)
	if errCode != protocol.ErrNone {
		s.Connections.Delete(connID)
		return
	}

	// 2. Wait for agent acknowledgment with timeout (ProtocolConn will be created in OnAck)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.Ctx.Done():
			s.SendClose(connID, protocol.ErrHandlerStopped)
			s.Connections.Delete(connID)
			return
		case <-timeout:
			s.SendClose(connID, protocol.ErrTransportTimeout)
			s.Connections.Delete(connID)
			return
		case <-ticker.C:
			if proxyConn.ProtocolConn != nil {
				// Agent acknowledged connection
				goto connected
			}
		}
	}

connected:
	// 3. Connection established, start bidirectional forwarding using io.Copy
	errCh := make(chan error, 2)

	// Client → Agent
	go func() {
		_, err := io.Copy(proxyConn.ProtocolConn, clientConn)
		errCh <- err
	}()

	// Agent → Client
	go func() {
		_, err := io.Copy(clientConn, proxyConn.ProtocolConn)
		errCh <- err
	}()

	// Wait for BOTH directions to complete before cleaning up
	var err1, err2 error
	for i := 0; i < 2; i++ {
		select {
		case <-s.Ctx.Done():
			s.cleanupConnection(connID, clientConn, proxyConn)
			return
		case <-proxyConn.Closed:
			s.cleanupConnection(connID, clientConn, proxyConn)
			return
		case err := <-errCh:
			if err1 == nil {
				err1 = err
			} else {
				err2 = err
			}
		}
	}

	// Both directions finished - NOW clean up
	s.cleanupConnection(connID, clientConn, proxyConn)

	// Log errors if any
	for _, err := range []error{err1, err2} {
		if err != nil && !errors.Is(err, io.EOF) {
			log.Debug().Err(err).Str("conn_id", connID.String()).Msg("Connection closed with error")
		}
	}
}
