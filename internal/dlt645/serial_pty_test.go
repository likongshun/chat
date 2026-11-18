//go:build linux

package dlt645

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/creack/pty"
)

func openPTYSession(t *testing.T, ctx context.Context, timeout time.Duration) (*os.File, Session) {
	t.Helper()
	master, slave, err := pty.Open()
	if err != nil {
		t.Skipf("PTY is unavailable: %v", err)
	}
	t.Cleanup(func() { _ = master.Close(); _ = slave.Close() })
	endpoint := fmt.Sprintf("serial://%s?baud=9600&data_bits=8&parity=none&stop_bits=1", slave.Name())
	session, err := (SerialTransport{}).Open(ctx, endpoint, timeout)
	if err != nil {
		t.Fatalf("open serial PTY session: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return master, session
}

func TestSerialTransportPTYRoundTrip(t *testing.T) {
	master, session := openPTYSession(t, context.Background(), time.Second)
	request := []byte{0xFE, 0xFE, 0x68, 0x16}
	if _, err := session.Write(request); err != nil {
		t.Fatalf("serial write: %v", err)
	}
	got := make([]byte, len(request))
	if _, err := io.ReadFull(master, got); err != nil {
		t.Fatalf("read request from PTY master: %v", err)
	}
	if !bytes.Equal(got, request) {
		t.Fatalf("request mismatch: got % X want % X", got, request)
	}

	reply := []byte{0x68, 0x91, 0x00, 0x16}
	if _, err := master.Write(reply); err != nil {
		t.Fatalf("write reply to PTY master: %v", err)
	}
	if err := session.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	got = make([]byte, len(reply))
	if _, err := io.ReadFull(session, got); err != nil {
		t.Fatalf("read serial reply: %v", err)
	}
	if !bytes.Equal(got, reply) {
		t.Fatalf("reply mismatch: got % X want % X", got, reply)
	}
}

func TestSerialTransportPTYReadDeadline(t *testing.T) {
	_, session := openPTYSession(t, context.Background(), time.Second)
	if err := session.SetReadDeadline(time.Now().Add(60 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	buf := make([]byte, 1)
	_, err := session.Read(buf)
	if err == nil {
		t.Fatal("expected read timeout")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) && time.Since(start) < 40*time.Millisecond {
		t.Fatalf("read returned too early without deadline error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("read deadline was not enforced: %s", elapsed)
	}
}

func TestSerialTransportPTYContextCancellationClosesSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, session := openPTYSession(t, ctx, time.Second)
	result := make(chan error, 1)
	go func() {
		_, err := session.Read(make([]byte, 1))
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("read unexpectedly succeeded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not unblock serial read")
	}
}

func TestSerialTransportPTYCanReopenAfterSessionClose(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Skipf("PTY is unavailable: %v", err)
	}
	defer master.Close()
	defer slave.Close()
	endpoint := fmt.Sprintf("serial://%s?baud=9600&data_bits=8&parity=none&stop_bits=1", slave.Name())
	first, err := (SerialTransport{}).Open(context.Background(), endpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := (SerialTransport{}).Open(context.Background(), endpoint, time.Second)
	if err != nil {
		t.Fatalf("reopen serial port after session close: %v", err)
	}
	defer second.Close()
	payload := []byte{0x68, 0x11, 0x16}
	if _, err := second.Write(payload); err != nil {
		t.Fatalf("write after reopen: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(master, got); err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reopened session wrote % X, want % X", got, payload)
	}
}
