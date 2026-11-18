package dlt645

import (
	"context"
	"net"
	"time"
)

// Session is one exclusive half-duplex bus session. Serial adapters can
// implement this interface without changing the DL/T645 poller.
type Session interface {
	net.Conn
}

// Transport opens a single meter-bus session. A session must not be shared
// concurrently; the poller serializes access by physical gateway/port.
type Transport interface {
	Open(ctx context.Context, endpoint string, timeout time.Duration) (Session, error)
}

// TCPTransport connects to a transparent TCP-to-RS485 gateway.
type TCPTransport struct{}

func (TCPTransport) Open(ctx context.Context, endpoint string, timeout time.Duration) (Session, error) {
	if timeout <= 0 { timeout = 1500 * time.Millisecond }
	conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", endpoint)
	if err != nil { return nil, err }
	return conn, nil
}
