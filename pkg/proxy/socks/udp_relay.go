package proxy

import (
	"net"
	"time"
)

// UDPRelayConn is a UDP socket that can send and receive datagrams to/from
// arbitrary addresses. Used by the SOCKS5 UDP ASSOCIATE handler.
// Implementations must be safe for concurrent use.
type UDPRelayConn interface {
	// LocalPort returns the port this socket is bound to.
	LocalPort() int

	// ReadFrom reads a datagram into b, returning the sender's address.
	// Blocks until data arrives, the socket is closed, or the read deadline fires.
	ReadFrom(b []byte) (int, *net.UDPAddr, error)

	// WriteTo sends a datagram to addr.
	WriteTo(b []byte, addr *net.UDPAddr) error

	// SetReadDeadline sets the deadline for future ReadFrom calls.
	// A zero value disables the deadline.
	SetReadDeadline(t time.Time) error

	// Close closes the socket.
	Close() error
}
