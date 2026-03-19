//go:build js

package proxy

import (
	"fmt"
	"net"
	"os"
	"sync"
	"syscall/js"
	"time"
)

type jsUDPPacket struct {
	data []byte
	addr *net.UDPAddr
}

type jsUDPConn struct {
	socket  js.Value
	port    int
	packets chan jsUDPPacket
	closed  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	dl      time.Time // read deadline
}

func listenUDP() (UDPRelayConn, error) {
	c := &jsUDPConn{
		packets: make(chan jsUDPPacket, 256),
		closed:  make(chan struct{}),
	}

	done := make(chan error, 1)

	onBind := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c.socket = args[0]
		c.port = args[1].Int()
		select {
		case done <- nil:
		default:
		}
		return nil
	})

	onData := js.FuncOf(func(_ js.Value, args []js.Value) any {
		// args: socket, Uint8Array, port, address
		jsData := args[1]
		port := args[2].Int()
		host := args[3].String()

		data := make([]byte, jsData.Length())
		js.CopyBytesToGo(data, jsData)

		addr := &net.UDPAddr{IP: net.ParseIP(host), Port: port}
		select {
		case c.packets <- jsUDPPacket{data, addr}:
		case <-c.closed:
		default:
		}
		return nil
	})

	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
		msg := "udp socket error"
		if len(args) > 0 {
			msg = args[0].String()
		}
		select {
		case done <- fmt.Errorf("%s", msg):
		default:
		}
		return nil
	})

	js.Global().Call("UDPListen", onBind, onData, onError)

	if err := <-done; err != nil {
		return nil, err
	}
	return c, nil
}

func (c *jsUDPConn) LocalPort() int { return c.port }

func (c *jsUDPConn) ReadFrom(b []byte) (int, *net.UDPAddr, error) {
	c.mu.Lock()
	dl := c.dl
	c.mu.Unlock()

	var timer <-chan time.Time
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: os.ErrDeadlineExceeded}
		}
		timer = time.After(d)
	}

	select {
	case pkt := <-c.packets:
		n := copy(b, pkt.data)
		return n, pkt.addr, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	case <-timer:
		return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: os.ErrDeadlineExceeded}
	}
}

func (c *jsUDPConn) WriteTo(b []byte, addr *net.UDPAddr) error {
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}
	jsData := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(jsData, b)
	c.socket.Call("send", jsData, addr.Port, addr.IP.String())
	return nil
}

func (c *jsUDPConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.dl = t
	c.mu.Unlock()
	return nil
}

func (c *jsUDPConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		c.socket.Call("close")
	})
	return nil
}
