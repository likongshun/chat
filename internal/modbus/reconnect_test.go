package modbus

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientEmitsReconnectOnlyAfterSuccessfulRedial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var accepted atomic.Int32
	serverDone := make(chan error, 1)
	go func() {
		first, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		accepted.Add(1)
		var header [7]byte
		if _, err := io.ReadFull(first, header[:]); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		length := int(binary.BigEndian.Uint16(header[4:6])) - 1
		request := make([]byte, length)
		if _, err := io.ReadFull(first, request); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		// Simulate a peer restart after accepting the first request.
		_ = first.Close()

		second, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer second.Close()
		accepted.Add(1)
		if _, err := io.ReadFull(second, header[:]); err != nil {
			serverDone <- err
			return
		}
		length = int(binary.BigEndian.Uint16(header[4:6])) - 1
		request = make([]byte, length)
		if _, err := io.ReadFull(second, request); err != nil {
			serverDone <- err
			return
		}
		// FC03, one register with value 42.
		response := make([]byte, 11)
		copy(response[:2], header[:2])
		binary.BigEndian.PutUint16(response[4:6], 5)
		response[6] = header[6]
		response[7] = 3
		response[8] = 2
		binary.BigEndian.PutUint16(response[9:11], 42)
		_, err = second.Write(response)
		serverDone <- err
	}()

	var reconnects atomic.Int32
	client := &Client{
		Address: ln.Addr().String(),
		Timeout: time.Second,
		OnReconnect: func() { reconnects.Add(1) },
	}
	defer client.Close()

	if _, err := client.ReadHolding(context.Background(), 1, 0, 1); err == nil {
		t.Fatal("first request should fail after peer closes the socket")
	}
	if reconnects.Load() != 0 {
		t.Fatalf("reconnect event emitted before a successful redial: %d", reconnects.Load())
	}
	values, err := client.ReadHolding(context.Background(), 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != 42 {
		t.Fatalf("unexpected response after reconnect: %v", values)
	}
	if reconnects.Load() != 1 {
		t.Fatalf("reconnect events=%d, want 1", reconnects.Load())
	}
	if accepted.Load() != 2 {
		t.Fatalf("accepted connections=%d, want 2", accepted.Load())
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
