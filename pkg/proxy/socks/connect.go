package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"proxyblob/pkg/protocol"
)

// handleConnect processes the SOCKS5 CONNECT command.
// It establishes a TCP connection to the requested target and
// sets up bidirectional data transfer between client and target.
//
// The CONNECT command format is:
//
//	+-----+-----+-----+------+----------+----------+
//	| VER | CMD | RSV | ATYP | DST.ADDR | DST.PORT |
//	+-----+-----+-----+------+----------+----------+
//	|  1  |  1  |  1  |  1   | Variable |    2     |
//
// Returns an error code indicating success or specific failure reason.
func (h *SocksHandler) handleConnect(conn *protocol.Connection, cmdData []byte) byte {
	if len(cmdData) < 4 {
		// Send malformed request response
		response := []byte{Version5, GeneralFailure, 0x00, IPv4, 0, 0, 0, 0, 0, 0}
		h.SendData(conn.ID, response)
		return protocol.ErrAddressNotSupported
	}

	// Parse target address
	target, errCode := ParseAddress(cmdData[3:])
	if errCode != protocol.ErrNone {
		h.SendError(conn, errCode)
		return errCode
	}

	fmt.Printf("[CONNECT] %s\n", target)

	// Establish TCP connection to target
	targetConn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		// Map network error to appropriate protocol error code
		errCode = protocol.MapNetError(err)
		h.SendError(conn, errCode)
		return errCode
	}

	// Enable TCP_NODELAY to disable Nagle's algorithm for better TLS performance
	if tcpConn, ok := targetConn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	// Send success response
	// Use stack allocation for fixed-size response (10 bytes)
	localAddr := targetConn.LocalAddr().(*net.TCPAddr)
	var responseBuf [10]byte
	responseBuf[0] = Version5
	responseBuf[1] = Succeeded
	responseBuf[2] = 0x00
	responseBuf[3] = IPv4
	copy(responseBuf[4:8], localAddr.IP.To4())
	binary.BigEndian.PutUint16(responseBuf[8:], uint16(localAddr.Port))
	response := responseBuf[:]

	errCode = h.SendData(conn.ID, response)
	if errCode != protocol.ErrNone {
		targetConn.Close()
		return protocol.ErrPacketSendFailed
	}

	// Store connection (state is already established via ProtocolConn)
	conn.Conn = targetConn

	// Start data transfer
	return h.handleTCPDataTransfer(conn, targetConn)
}

// handleTCPDataTransfer manages bidirectional data transfer for TCP connections.
// Uses io.Copy for efficient data transfer: io.Copy(dst, src)
//
// The transfer continues until either:
//   - The connection is closed by either end
//   - The context is canceled
//   - An error occurs
func (h *SocksHandler) handleTCPDataTransfer(conn *protocol.Connection, tcpConn net.Conn) byte {
	errCh := make(chan error, 2)

	// Client → Target
	go func() {
		_, err := io.Copy(tcpConn, conn.ProtocolConn)
		// Half-close: close write side of target when client→target finishes
		if tcpCloser, ok := tcpConn.(*net.TCPConn); ok {
			tcpCloser.CloseWrite()
		}
		errCh <- err
	}()

	// Target → Client
	go func() {
		_, err := io.Copy(conn.ProtocolConn, tcpConn)
		errCh <- err
	}()

	// Wait for BOTH directions to complete (don't exit early on first error!)
	// DO NOT listen to conn.Closed here - it will abort the wait loop prematurely!
	var err1, err2 error
	for i := 0; i < 2; i++ {
		select {
		case <-h.Ctx.Done():
			tcpConn.Close()
			conn.ProtocolConn.Close()
			return protocol.ErrHandlerStopped

		case <-conn.Closed:
			tcpConn.Close()
			conn.ProtocolConn.Close()
			// Drain any remaining errors
			select {
			case <-errCh:
			default:
			}
			return protocol.ErrNone

		case err := <-errCh:
			if err1 == nil {
				err1 = err
			} else {
				err2 = err
			}
		}
	}

	// Both directions finished - NOW it's safe to close
	tcpConn.Close()
	conn.ProtocolConn.Close()

	// Check for errors (ignore normal EOF and closed connections)
	for _, err := range []error{err1, err2} {
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			// Use helper to map network errors to protocol errors
			if errCode := protocol.MapNetError(err); errCode != protocol.ErrTransportClosed {
				return errCode
			}
		}
	}

	return protocol.ErrNone
}
