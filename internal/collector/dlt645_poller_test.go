package collector

import (
	"context"
	"io"
	"net"
	"math"
	"time"
	"testing"

	"github.com/likongshun/chat/internal/dlt645"
)

func TestDecode645ValueBCDAndScale(t *testing.T) {
	got, err := decode645Value([]byte{0x00, 0x00, 0x01, 0x00, 0x34, 0x12}, "00010000", "BCD", 2, 0.01, 0)
	if err != nil { t.Fatal(err) }
	if math.Abs(got-12.34) > 1e-9 { t.Fatalf("got %v want 12.34", got) }
}

func TestDecode645ValueRejectsBadBCDAndIdentifier(t *testing.T) {
	if _, err := decode645Value([]byte{0, 0, 1, 0, 0xfa}, "00010000", "BCD", 1, 1, 0); err == nil { t.Fatal("expected invalid BCD") }
	if _, err := decode645Value([]byte{0, 0, 1, 0, 1}, "00020000", "BCD", 1, 1, 0); err == nil { t.Fatal("expected data identifier mismatch") }
}

func TestValidateReadResponseControl(t *testing.T) {
	if err := validateReadResponseControl(0x91); err != nil { t.Fatal(err) }
	if err := validateReadResponseControl(0xB1); err != nil { t.Fatal("continuation response rejected:", err) }
	for _, control := range []byte{0x11, 0xD1, 0x92} {
		if err := validateReadResponseControl(control); err == nil { t.Fatalf("control 0x%02x unexpectedly accepted", control) }
	}
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	w := &shortWriter{max:2}
	if err := writeAll(w, []byte{1,2,3,4,5}); err != nil { t.Fatal(err) }
	if string(w.data) != string([]byte{1,2,3,4,5}) { t.Fatalf("unexpected output %v", w.data) }
}

type shortWriter struct { data []byte; max int }
func (w *shortWriter) Write(p []byte) (int,error) {
	if len(p) > w.max { p = p[:w.max] }
	w.data = append(w.data, p...)
	return len(p),nil
}

func TestDecode645ValueLengthAndBCD(t *testing.T) {
	got, err := decode645Value([]byte{0x00,0x00,0x01,0x00,0x45,0x23,0x01}, "00010000", "BCD", 3, 0.01, 0)
	if err != nil { t.Fatal(err) }
	if got != 123.45 { t.Fatalf("got %v, want 123.45", got) }
	if _, err := decode645Value([]byte{0x00,0x00,0x01,0x00,0x45}, "00010000", "BCD", 3, 1, 0); err == nil { t.Fatal("expected length mismatch") }
	if _, err := decode645Value([]byte{0x00,0x00,0x01,0x00,0x4a,0x23,0x01}, "00010000", "BCD", 3, 1, 0); err == nil { t.Fatal("expected invalid BCD") }
}

func TestGatewayAcquisitionHonorsContextCancellation(t *testing.T) {
	p := &DL645Poller{}
	release, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.acquireGateway(ctx, "gateway:4001"); err == nil { t.Fatal("expected canceled acquisition") }
	release()
	next, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	next()
}

func TestGatewayAcquisitionSerializesAndReleases(t *testing.T) {
	p := &DL645Poller{}
	release, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.acquireGateway(ctx, "gateway:4001"); err == nil { t.Fatal("expected serialized acquisition timeout") }
	release()
	next, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	next()
}

func TestRead645PayloadAssemblesFollowFrames(t *testing.T) {
	address, dataID := "123456789012", "00010000"
	firstData := []byte{0x00,0x00,0x01,0x00,0x12}
	firstRaw := test645RawFrame(address, 0xB1, add645Offset(firstData))
	first, err := dlt645.DecodeResponse(firstRaw, address)
	if err != nil { t.Fatal(err) }
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	serverErr := make(chan error, 1)
	go func() {
		for seq, control := range []byte{0xB2,0x92} {
			request := make([]byte, 17)
			if _, err := io.ReadFull(peer, request); err != nil { serverErr <- err; return }
			if request[8] != dlt645.ReadFollow || request[9] != 5 || request[14] != byte(seq+1)+0x33 { serverErr <- fmt.Errorf("bad continuation request %x", request); return }
			data := []byte{0x00,0x00,0x01,0x00,byte(0x34+seq),byte(seq+1)}
			if err := writeAll(peer, test645RawFrame(address, control, add645Offset(data))); err != nil { serverErr <- err; return }
		}
		serverErr <- nil
	}()
	got, err := read645Payload(client, first, address, dataID, time.Second, context.Background())
	if err != nil { t.Fatal(err) }
	if len(got) != 7 || got[4] != 0x12 || got[5] != 0x34 || got[6] != 0x35 { t.Fatalf("unexpected assembled payload %x", got) }
	if err := <-serverErr; err != nil { t.Fatal(err) }
}

func TestRead645PayloadRejectsSequenceMismatch(t *testing.T) {
	address, dataID := "123456789012", "00010000"
	first, err := dlt645.DecodeResponse(test645RawFrame(address, 0xB1, add645Offset([]byte{0,0,1,0,0x12})), address)
	if err != nil { t.Fatal(err) }
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	go func() {
		request := make([]byte, 17)
		_, _ = io.ReadFull(peer, request)
		bad := []byte{0,0,1,0,0x34,0x35} // sequence 2 while sequence 1 is expected
		_ = writeAll(peer, test645RawFrame(address, 0x92, add645Offset(bad)))
	}()
	if _, err := read645Payload(client, first, address, dataID, time.Second, context.Background()); err == nil { t.Fatal("expected sequence mismatch") }
}

func add645Offset(data []byte) []byte {
	out := append([]byte(nil), data...)
	for i := range out { out[i] += 0x33 }
	return out
}

func test645RawFrame(address string, control byte, data []byte) []byte {
	addr := make([]byte, 6)
	for i := 0; i < 6; i++ { addr[i] = (address[10-2*i]-'0')<<4 | (address[11-2*i]-'0') }
	out := []byte{0x68}
	out = append(out, addr...)
	out = append(out, 0x68, control, byte(len(data)))
	out = append(out, data...)
	var sum byte
	for _, b := range out { sum += b }
	return append(out, sum, 0x16)
}
