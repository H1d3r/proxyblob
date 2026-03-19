//go:build js

package proxy

import (
	"fmt"
	"io"
	"net"
	"syscall/js"
	"time"
)

type jsConn struct {
	socket js.Value
	pr     *io.PipeReader
	pw     *io.PipeWriter
	closed chan struct{}
}

func dialTCP(target string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	conn := &jsConn{
		pr:     pr,
		pw:     pw,
		closed: make(chan struct{}),
	}

	done := make(chan error, 1)

	onConnect := js.FuncOf(func(_ js.Value, args []js.Value) any {
		conn.socket = args[0]
		select {
		case done <- nil:
		default:
		}
		return nil
	})

	onData := js.FuncOf(func(_ js.Value, args []js.Value) any {
		jsData := args[1]
		data := make([]byte, jsData.Length())
		js.CopyBytesToGo(data, jsData)
		go pw.Write(data) // don't block the JS callback
		return nil
	})

	onClose := js.FuncOf(func(_ js.Value, args []js.Value) any {
		pw.CloseWithError(io.EOF)
		select {
		case <-conn.closed:
		default:
			close(conn.closed)
		}
		return nil
	})

	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
		msg := "connection error"
		if len(args) > 0 && args[0].Type() == js.TypeString {
			msg = args[0].String()
		}
		e := fmt.Errorf("%s", msg)
		pw.CloseWithError(e)
		select {
		case done <- e:
		default:
		}
		return nil
	})

	js.Global().Call("TCPDial", host, port, onConnect, onData, onClose, onError)

	if err := <-done; err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *jsConn) Read(b []byte) (int, error)  { return c.pr.Read(b) }

func (c *jsConn) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	jsData := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(jsData, b)
	c.socket.Call("write", jsData)
	return len(b), nil
}

func (c *jsConn) Close() error {
	select {
	case <-c.closed:
		return nil
	default:
		close(c.closed)
		c.pr.CloseWithError(net.ErrClosed)
		c.pw.CloseWithError(net.ErrClosed)
		c.socket.Call("end")
		return nil
	}
}

func (c *jsConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *jsConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *jsConn) SetDeadline(t time.Time) error      { return nil }
func (c *jsConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *jsConn) SetWriteDeadline(t time.Time) error { return nil }
