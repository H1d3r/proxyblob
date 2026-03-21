//go:build !js

package proxy

import (
	"net"
	"time"
)

func dialTCP(target string) (net.Conn, error) {
	return net.DialTimeout("tcp", target, 10*time.Second)
}
