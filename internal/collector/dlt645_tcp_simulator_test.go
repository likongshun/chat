package collector

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/likongshun/chat/internal/config"
)

// This is a wire-level TCP-to-RS485 gateway simulator. It deliberately speaks
// real DL/T645 frames so the poller, stream framing and continuation logic are
// exercised together rather than through net.Pipe or a mocked Transport.
func startDLT645TCPMeter(t *testing.T, corruptChecksum bool) (string, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

		request := make([]byte, 17)
		if _, err := io.ReadFull(conn, request); err != nil {
			result <- err
			return
		}
		if request[0] != 0x68 || request[7] != 0x68 || request[8] != 0x11 {
			result <- io.ErrUnexpectedEOF
			return
		}
		// DI=00010000, with the least-significant byte transmitted first.
		di := []byte{0x00, 0x00, 0x01, 0x00}
		first := append(append([]byte(nil), di...), 0x45)
		frame := simulator645Frame("123456789012", 0xB1, first)
		if corruptChecksum {
			frame[len(frame)-2]++
		}
		if _, err := conn.Write(frame); err != nil {
			result <- err
			return
		}
		if corruptChecksum {
			result <- nil
			return
		}

		follow := make([]byte, 17)
		if _, err := io.ReadFull(conn, follow); err != nil {
			result <- err
			return
		}
		if follow[8] != 0x12 || follow[14] != 0x34 {
			result <- io.ErrUnexpectedEOF
			return
		}
		last := append(append([]byte(nil), di...), 0x23, 0x01, 0x34)
		if _, err := conn.Write(simulator645Frame("123456789012", 0x92, last)); err != nil {
			result <- err
			return
		}
		result <- nil
	}()
	return ln.Addr().String(), result
}

func simulator645Frame(address string, control byte, data []byte) []byte {
	addr := make([]byte, 6)
	for i := 0; i < 6; i++ {
		addr[i] = (address[10-2*i]-'0')<<4 | (address[11-2*i] - '0')
	}
	out := []byte{0x68}
	out = append(out, addr...)
	out = append(out, 0x68, control, byte(len(data)))
	for _, b := range data {
		out = append(out, b+0x33)
	}
	var checksum byte
	for _, b := range out {
		checksum += b
	}
	return append(append(out, checksum), 0x16)
}

func TestDLT645PollerOverRealTCPGatewayWithFollowFrames(t *testing.T) {
	address, serverDone := startDLT645TCPMeter(t, false)
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	var port uint16
	for _, c := range portText {
		port = port*10 + uint16(c-'0')
	}
	poller := &DL645Poller{
		Config: config.Config{Points: []config.Point{{
			Model: "meter-v1", PointCode: "energy", DataID: "00010000",
			DataType: "BCD", Count: 3, Scale: 0.01, Enabled: true,
		}}},
		Timeout: time.Second,
	}
	device := config.Device{
		ID: "meter-1", Protocol: "dl-t645-2007", Host: host, Port: port,
		MeterAddress: "123456789012", Model: "meter-v1", Enabled: true,
	}
	samples, err := poller.Poll(context.Background(), device)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 || samples[0].Quality != QualityGood || samples[0].Value != 123.45 {
		t.Fatalf("unexpected simulated meter sample: %#v", samples)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestDLT645PollerOverRealTCPRejectsChecksumCorruption(t *testing.T) {
	address, serverDone := startDLT645TCPMeter(t, true)
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	var port uint16
	for _, c := range portText {
		port = port*10 + uint16(c-'0')
	}
	poller := &DL645Poller{
		Config: config.Config{Points: []config.Point{{
			Model: "meter-v1", PointCode: "energy", DataID: "00010000",
			DataType: "BCD", Count: 3, Scale: 0.01, Enabled: true,
		}}},
		Timeout: time.Second,
	}
	device := config.Device{
		ID: "meter-1", Protocol: "dl-t645-2007", Host: host, Port: port,
		MeterAddress: "123456789012", Model: "meter-v1", Enabled: true,
	}
	samples, err := poller.Poll(context.Background(), device)
	if err == nil || len(samples) != 1 || samples[0].Quality != QualityCommError {
		t.Fatalf("expected checksum communication error, samples=%#v err=%v", samples, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

// Keep encoding/binary imported here as an explicit wire-layout assertion:
// the MBAP-free DL/T645 frame uses a one-byte data length at offset 9.
func TestSimulator645FrameLengthField(t *testing.T) {
	frame := simulator645Frame("123456789012", 0x91, []byte{1, 2, 3})
	if got := int(frame[9]); got != 3 {
		t.Fatalf("DL/T645 length field=%d, want 3", got)
	}
}
