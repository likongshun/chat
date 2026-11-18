package dlt645

import (
	"bytes"
	"testing"
)

func TestEncodeReadDataAndDecodeResponse(t *testing.T) {
	req, err := EncodeReadData("123456789012", "00010000")
	if err != nil { t.Fatal(err) }
	if len(req) != 16 || req[0] != FrameStart || req[7] != FrameStart || req[8] != ReadData || req[len(req)-1] != FrameEnd { t.Fatalf("bad request: %x", req) }
	if !bytes.Equal(req[10:14], []byte{0x33,0x33,0x34,0x33}) { t.Fatalf("unexpected DI wire order: %x", req[10:14]) }
	// Response payload is data ID + one BCD byte, with 0x33 added on wire.
	payload := []byte{0x33,0x34,0x33,0x33,0x34}
	wirePayload := append([]byte(nil), payload...)
	for i := range wirePayload { wirePayload[i] += 0x33 }
	addr, _ := encodeAddress("123456789012")
	raw := encodeFrame(addr, 0x91, wirePayload)
	got, err := DecodeResponse(append([]byte{0xfe,0xfe}, raw...), "123456789012")
	if err != nil { t.Fatal(err) }
	if got.Control != 0x91 || !bytes.Equal(got.Data, payload) { t.Fatalf("unexpected frame: %+v", got) }
}

func TestDecodeRejectsChecksumAndAddress(t *testing.T) {
	addr, _ := encodeAddress("123456789012")
	raw := encodeFrame(addr, 0x91, []byte{0x33})
	bad := append([]byte(nil), raw...)
	bad[len(bad)-2]++
	if _, err := DecodeResponse(bad, "123456789012"); err != ErrChecksum { t.Fatalf("expected checksum error, got %v", err) }
	if _, err := DecodeResponse(raw, "000000000001"); err == nil { t.Fatal("expected address mismatch") }
}

func TestEncodeRejectsInvalidInputs(t *testing.T) {
	if _, err := EncodeReadData("123", "00010000"); err == nil { t.Fatal("expected address validation") }
	if _, err := EncodeReadData("123456789012", "1234"); err == nil { t.Fatal("expected data ID validation") }
}

func TestEncodeReadFollow(t *testing.T) {
	req, err := EncodeReadFollow("123456789012", "00010000", 1)
	if err != nil { t.Fatal(err) }
	if req[8] != ReadFollow || req[9] != 5 || req[len(req)-1] != FrameEnd { t.Fatalf("bad follow request: %x", req) }
	if !bytes.Equal(req[10:15], []byte{0x33,0x33,0x34,0x33,0x34}) { t.Fatalf("unexpected follow payload: %x", req[10:15]) }
}
