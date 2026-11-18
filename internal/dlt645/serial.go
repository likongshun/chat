package dlt645

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.bug.st/serial"
)

// SerialTransport opens a local serial device. The endpoint format is
// serial:///dev/ttyUSB0?baud=9600&data_bits=8&parity=even&stop_bits=1.
// USB-RS485 adapters with automatic direction control are supported; adapters
// requiring kernel RS-485 direction GPIO/ioctl need a platform-specific driver.
type SerialTransport struct{}

func (SerialTransport) Open(ctx context.Context, endpoint string, timeout time.Duration) (Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	device, mode, err := parseSerialEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	port, err := serial.Open(device, mode)
	if err != nil {
		return nil, fmt.Errorf("open serial device %s: %w", device, err)
	}
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	if err := port.SetReadTimeout(timeout); err != nil {
		_ = port.Close()
		return nil, fmt.Errorf("set serial read timeout: %w", err)
	}
	s := &serialSession{port: port, closed: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-s.closed:
		}
	}()
	return s, nil
}

func parseSerialEndpoint(endpoint string) (string, *serial.Mode, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "serial" || u.Host != "" || u.User != nil || u.Fragment != "" {
		return "", nil, fmt.Errorf("invalid serial endpoint %q", endpoint)
	}
	device := filepath.Clean(u.Path)
	if !filepath.IsAbs(device) || !strings.HasPrefix(device, "/dev/") || device == "/dev" {
		return "", nil, fmt.Errorf("serial device path must be an absolute /dev path")
	}
	for _, part := range strings.Split(u.Path, "/") {
		if part == ".." {
			return "", nil, fmt.Errorf("serial device path traversal is not allowed")
		}
	}
	for key := range u.Query() {
		switch key {
		case "baud", "data_bits", "parity", "stop_bits":
		default:
			return "", nil, fmt.Errorf("unknown serial option %q", key)
		}
	}
	q := u.Query()
	baud, err := queryInt(q.Get("baud"), 9600)
	if err != nil || baud < 300 || baud > 4000000 {
		return "", nil, fmt.Errorf("serial baud must be an integer in 300..4000000")
	}
	dataBits, err := queryInt(q.Get("data_bits"), 8)
	if err != nil || dataBits < 5 || dataBits > 8 {
		return "", nil, fmt.Errorf("serial data_bits must be 5..8")
	}
	stopBits, err := queryInt(q.Get("stop_bits"), 1)
	if err != nil || (stopBits != 1 && stopBits != 2) {
		return "", nil, fmt.Errorf("serial stop_bits must be 1 or 2")
	}
	mode := &serial.Mode{BaudRate: baud, DataBits: dataBits, StopBits: serial.OneStopBit}
	if stopBits == 2 {
		mode.StopBits = serial.TwoStopBits
	}
	switch strings.ToLower(q.Get("parity")) {
	case "", "none", "n":
		mode.Parity = serial.NoParity
	case "even", "e":
		mode.Parity = serial.EvenParity
	case "odd", "o":
		mode.Parity = serial.OddParity
	default:
		return "", nil, fmt.Errorf("serial parity must be none, even or odd")
	}
	return device, mode, nil
}

type serialSession struct {
	port serial.Port
	closed chan struct{}
	deadlineMu sync.Mutex
	readDeadline time.Time
	closeOnce sync.Once
	closeErr error
}

func (s *serialSession) Read(p []byte) (int, error) {
	s.deadlineMu.Lock()
	deadline := s.readDeadline
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			s.deadlineMu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		if err := s.port.SetReadTimeout(remaining); err != nil {
			s.deadlineMu.Unlock()
			return 0, err
		}
	}
	s.deadlineMu.Unlock()
	n, err := s.port.Read(p)
	if n == 0 && err == nil {
		s.deadlineMu.Lock()
		expired := !s.readDeadline.IsZero() && !time.Now().Before(s.readDeadline)
		s.deadlineMu.Unlock()
		if expired {
			return 0, os.ErrDeadlineExceeded
		}
	}
	return n, err
}
func (s *serialSession) Write(p []byte) (int, error) {
	n, err := s.port.Write(p)
	if err == nil && n > 0 {
		// Drain before switching to receive mode; this prevents a half-duplex
		// RS485 adapter from truncating the tail of a request still in flight.
		err = s.port.Drain()
	}
	return n, err
}
func (s *serialSession) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.closeErr = s.port.Close()
	})
	return s.closeErr
}
func (s *serialSession) LocalAddr() net.Addr                { return serialAddr("local") }
func (s *serialSession) RemoteAddr() net.Addr               { return serialAddr("serial") }
func (s *serialSession) SetDeadline(t time.Time) error      { return s.SetReadDeadline(t) }
func (s *serialSession) SetWriteDeadline(time.Time) error   { return nil }
func (s *serialSession) SetReadDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.readDeadline = t
	s.deadlineMu.Unlock()
	d := time.Until(t)
	if t.IsZero() {
		d = serial.NoTimeout
	} else if d <= 0 {
		d = time.Nanosecond
	}
	return s.port.SetReadTimeout(d)
}

type serialAddr string
func (a serialAddr) Network() string { return "serial" }
func (a serialAddr) String() string  { return string(a) }

func queryInt(value string, fallback int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}
