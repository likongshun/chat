package modbus

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func serveModbusReply(t *testing.T, reply func([]byte) []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		var header [7]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		length := int(binary.BigEndian.Uint16(header[4:6])) - 1
		if length < 1 || length > 253 {
			return
		}
		pdu := make([]byte, length)
		if _, err := io.ReadFull(conn, pdu); err != nil {
			return
		}
		if response := reply(header[:]); response != nil {
			_, _ = conn.Write(response)
		}
	}()
	return ln.Addr().String()
}

func modbusReplyHeader(request []byte, txDelta uint16, protocol uint16, unit byte, length uint16, pdu ...byte) []byte {
	response := make([]byte, 7+len(pdu))
	binary.BigEndian.PutUint16(response[0:2], binary.BigEndian.Uint16(request[0:2])+txDelta)
	binary.BigEndian.PutUint16(response[2:4], protocol)
	binary.BigEndian.PutUint16(response[4:6], length)
	response[6] = unit
	copy(response[7:], pdu)
	return response
}

func TestClientRejectsInvalidMBAPLengthOnWire(t *testing.T) {
	address := serveModbusReply(t, func(request []byte) []byte {
		return modbusReplyHeader(request, 0, 0, request[6], 1, 3)
	})
	client := &Client{Address: address, Timeout: time.Second}
	defer client.Close()
	if _, err := client.ReadHolding(context.Background(), 1, 0, 1); err == nil {
		t.Fatal("expected invalid MBAP length error")
	}
}

func TestClientRejectsTransactionMismatchOnWire(t *testing.T) {
	address := serveModbusReply(t, func(request []byte) []byte {
		return modbusReplyHeader(request, 1, 0, request[6], 4, 3, 2, 0, 1)
	})
	client := &Client{Address: address, Timeout: time.Second}
	defer client.Close()
	if _, err := client.ReadHolding(context.Background(), 1, 0, 1); err == nil {
		t.Fatal("expected transaction mismatch error")
	}
}

func TestClientReturnsModbusExceptionOnWire(t *testing.T) {
	address := serveModbusReply(t, func(request []byte) []byte {
		return modbusReplyHeader(request, 0, 0, request[6], 3, 0x83, 2)
	})
	client := &Client{Address: address, Timeout: time.Second}
	defer client.Close()
	if _, err := client.ReadHolding(context.Background(), 1, 0, 1); !errors.Is(err, ErrException) {
		t.Fatalf("expected Modbus exception, got %v", err)
	}
}

func TestClientTimesOutWhenWirePeerDoesNotReply(t *testing.T) {
	address := serveModbusReply(t, func([]byte) []byte { return nil })
	client := &Client{Address: address, Timeout: 40 * time.Millisecond}
	defer client.Close()
	started := time.Now()
	_, err := client.ReadHolding(context.Background(), 1, 0, 1)
	if err == nil {
		t.Fatal("expected read timeout")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected network timeout, got %T: %v", err, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}
}
