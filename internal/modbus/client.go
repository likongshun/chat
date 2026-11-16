package modbus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var ErrException = errors.New("modbus exception")
var ErrClosed = errors.New("modbus client closed")

// Client is a serialized, persistent Modbus TCP connection. One Client should
// be shared by a single device worker; requests on it are never concurrent.
type Client struct {
	Address string
	Timeout time.Duration
	KeepAlive time.Duration

	mu sync.Mutex
	conn net.Conn
	closed bool
	tx atomic.Uint32
}

func (c *Client) timeout() time.Duration {
	if c.Timeout <= 0 { return 2 * time.Second }
	return c.Timeout
}
func (c *Client) keepAlive() time.Duration {
	if c.KeepAlive <= 0 { return 30 * time.Second }
	return c.KeepAlive
}

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout:c.timeout(), KeepAlive:c.keepAlive()}
	return d.DialContext(ctx, "tcp", c.Address)
}

func (c *Client) discardConn() {
	if c.conn != nil { _ = c.conn.Close(); c.conn = nil }
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn == nil { return nil }
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) request(ctx context.Context, unit, fn byte, payload []byte) ([]byte, error) {
	if ctx == nil { return nil, errors.New("nil context") }
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed { return nil, ErrClosed }
	if c.Address == "" { return nil, errors.New("empty Modbus TCP address") }
	if c.conn == nil {
		conn, err := c.dial(ctx)
		if err != nil { return nil, fmt.Errorf("dial %s: %w", c.Address, err) }
		c.conn = conn
	}

	conn := c.conn
	deadline := time.Now().Add(c.timeout())
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) { deadline = d }
	if err := conn.SetDeadline(deadline); err != nil { c.discardConn(); return nil, err }
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
		close(cancelDone)
	})
	defer func() {
		if !stop() { <-cancelDone }
		_ = conn.SetDeadline(time.Time{})
	}()

	txid := uint16(c.tx.Add(1))
	pdu := make([]byte, 1+len(payload))
	pdu[0] = fn
	copy(pdu[1:], payload)
	// MBAP length counts Unit Identifier plus PDU.
	frame := make([]byte, 7+len(pdu))
	binary.BigEndian.PutUint16(frame[0:2], txid)
	binary.BigEndian.PutUint16(frame[2:4], 0)
	binary.BigEndian.PutUint16(frame[4:6], uint16(1+len(pdu)))
	frame[6] = unit
	copy(frame[7:], pdu)

	if err := writeFull(conn, frame); err != nil {
		c.discardConn()
		return nil, fmt.Errorf("write Modbus request: %w", err)
	}
	var header [7]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		c.discardConn()
		return nil, fmt.Errorf("read MBAP header: %w", err)
	}
	length := binary.BigEndian.Uint16(header[4:6])
	if length < 2 || length > 254 {
		c.discardConn()
		return nil, fmt.Errorf("invalid MBAP length %d", length)
	}
	if binary.BigEndian.Uint16(header[0:2]) != txid ||
		binary.BigEndian.Uint16(header[2:4]) != 0 || header[6] != unit {
		c.discardConn()
		return nil, errors.New("Modbus MBAP transaction/protocol/unit mismatch")
	}
	body := make([]byte, int(length)-1)
	if _, err := io.ReadFull(conn, body); err != nil {
		c.discardConn()
		return nil, fmt.Errorf("read Modbus PDU: %w", err)
	}
	if len(body) == 0 {
		c.discardConn()
		return nil, errors.New("empty Modbus PDU")
	}
	if body[0] == fn|0x80 {
		if len(body) != 2 {
			c.discardConn()
			return nil, errors.New("invalid Modbus exception response")
		}
		return nil, fmt.Errorf("%w: function=%d code=%d", ErrException, fn, body[1])
	}
	if body[0] != fn {
		c.discardConn()
		return nil, fmt.Errorf("function mismatch: requested=%d received=%d", fn, body[0])
	}
	return body[1:], nil
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil { return err }
		if n == 0 { return io.ErrShortWrite }
		p = p[n:]
	}
	return nil
}

func (c *Client) ReadCoils(ctx context.Context, unit byte, address, count uint16) ([]bool, error) {
	return c.readBits(ctx, unit, 1, address, count)
}
func (c *Client) ReadDiscreteInputs(ctx context.Context, unit byte, address, count uint16) ([]bool, error) {
	return c.readBits(ctx, unit, 2, address, count)
}
func (c *Client) readBits(ctx context.Context, unit, fn byte, address, count uint16) ([]bool, error) {
	if count == 0 || count > 2000 { return nil, errors.New("bit count must be 1..2000") }
	payload := make([]byte, 4)
	binary.BigEndian.PutUint16(payload[0:2], address)
	binary.BigEndian.PutUint16(payload[2:4], count)
	body, err := c.request(ctx, unit, fn, payload)
	if err != nil { return nil, err }
	byteCount := (int(count)+7)/8
	if len(body) != byteCount+1 || int(body[0]) != byteCount {
		return nil, fmt.Errorf("invalid bit response: expected %d data bytes", byteCount)
	}
	out := make([]bool, count)
	for i := range out { out[i] = body[1+i/8]&(1<<uint(i%8)) != 0 }
	return out, nil
}

func (c *Client) ReadHolding(ctx context.Context, unit byte, address, count uint16) ([]uint16, error) {
	return c.readRegisters(ctx, unit, 3, address, count)
}
func (c *Client) ReadInput(ctx context.Context, unit byte, address, count uint16) ([]uint16, error) {
	return c.readRegisters(ctx, unit, 4, address, count)
}
func (c *Client) readRegisters(ctx context.Context, unit, fn byte, address, count uint16) ([]uint16, error) {
	if count == 0 || count > 125 { return nil, errors.New("register count must be 1..125") }
	payload := make([]byte, 4)
	binary.BigEndian.PutUint16(payload[0:2], address)
	binary.BigEndian.PutUint16(payload[2:4], count)
	body, err := c.request(ctx, unit, fn, payload)
	if err != nil { return nil, err }
	expected := int(count)*2
	if len(body) != expected+1 || int(body[0]) != expected {
		return nil, fmt.Errorf("invalid register response: expected %d data bytes", expected)
	}
	out := make([]uint16, count)
	for i := range out { out[i] = binary.BigEndian.Uint16(body[1+i*2:]) }
	return out, nil
}
