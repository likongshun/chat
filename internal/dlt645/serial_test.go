package dlt645

import (
	"testing"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"go.bug.st/serial"
)

func TestParseSerialEndpoint(t *testing.T) {
	device, mode, err := parseSerialEndpoint("serial:///dev/ttyUSB0?baud=2400&data_bits=8&parity=even&stop_bits=1")
	if err != nil {
		t.Fatal(err)
	}
	if device != "/dev/ttyUSB0" {
		t.Fatalf("device=%q", device)
	}
	if mode.BaudRate != 2400 || mode.DataBits != 8 || mode.Parity != serial.EvenParity || mode.StopBits != serial.OneStopBit {
		t.Fatalf("unexpected serial mode: %#v", mode)
	}
}

func TestParseSerialEndpointDefaults(t *testing.T) {
	_, mode, err := parseSerialEndpoint("serial:///dev/serial/by-id/usb-rs485")
	if err != nil {
		t.Fatal(err)
	}
	if mode.BaudRate != 9600 || mode.DataBits != 8 || mode.Parity != serial.NoParity || mode.StopBits != serial.OneStopBit {
		t.Fatalf("unexpected defaults: %#v", mode)
	}
}

func TestParseSerialEndpointRejectsUnsafeOrInvalidValues(t *testing.T) {
	cases := []string{
		"tcp://127.0.0.1:502",
		"serial:///etc/passwd",
		"serial:///dev/../etc/passwd",
		"serial:///dev/ttyUSB0?baud=bad",
		"serial:///dev/ttyUSB0?parity=mark",
		"serial:///dev/ttyUSB0?data_bits=9",
		"serial:///dev/ttyUSB0?stop_bits=3",
		"serial:///dev/ttyUSB0?unknown=1",
	}
	for _, endpoint := range cases {
		t.Run(endpoint, func(t *testing.T) {
			if _, _, err := parseSerialEndpoint(endpoint); err == nil {
				t.Fatalf("expected endpoint rejection: %s", endpoint)
			}
		})
	}
}

type blockingSerialPort struct {
	closed chan struct{}
	once sync.Once
}

func newBlockingSerialPort() *blockingSerialPort {
	return &blockingSerialPort{closed: make(chan struct{})}
}
func (p *blockingSerialPort) SetMode(*serial.Mode) error { return nil }
func (p *blockingSerialPort) Read([]byte) (int, error) { <-p.closed; return 0, io.ErrClosedPipe }
func (p *blockingSerialPort) Write([]byte) (int, error) { <-p.closed; return 0, io.ErrClosedPipe }
func (p *blockingSerialPort) Drain() error { return nil }
func (p *blockingSerialPort) ResetInputBuffer() error { return nil }
func (p *blockingSerialPort) ResetOutputBuffer() error { return nil }
func (p *blockingSerialPort) SetDTR(bool) error { return nil }
func (p *blockingSerialPort) SetRTS(bool) error { return nil }
func (p *blockingSerialPort) GetModemStatusBits() (*serial.ModemStatusBits, error) {
	return &serial.ModemStatusBits{}, nil
}
func (p *blockingSerialPort) SetReadTimeout(time.Duration) error { return nil }
func (p *blockingSerialPort) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}
func (p *blockingSerialPort) Break(time.Duration) error { return nil }

func TestSerialSessionWriteDeadlineClosesBlockedPort(t *testing.T) {
	port := newBlockingSerialPort()
	session := &serialSession{port: port, closed: make(chan struct{})}
	if err := session.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := session.Write([]byte{0x68})
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write error = %v, want deadline exceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("write deadline was not enforced")
	}
}
