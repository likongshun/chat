package collector

import (
	"context"
	"math"
	"time"
	"testing"
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
	for _, control := range []byte{0x11, 0xB1, 0xD1, 0x92} {
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
