package dlt645

import (
	"bytes"
	"testing"
	"time"
)

func TestEncodeReadLoadRecordByBlockCount(t *testing.T) {
	raw, err := EncodeReadLoadRecord("123456789012", "06010000", 6, nil)
	if err != nil { t.Fatal(err) }
	frame, err := DecodeResponse(raw, "123456789012")
	if err != nil { t.Fatal(err) }
	if frame.Control != ReadData { t.Fatalf("control=0x%02x, want 0x11", frame.Control) }
	want := []byte{0x00, 0x00, 0x01, 0x06, 0x06}
	if !bytes.Equal(frame.Data, want) { t.Fatalf("payload=%x, want %x", frame.Data, want) }
	if len(raw) != 17 || raw[9] != 5 { t.Fatalf("unexpected frame length/data length: %x", raw) }
}

func TestEncodeReadLoadRecordFromMeterLocalTime(t *testing.T) {
	start := time.Date(2026, time.November, 20, 13, 16, 0, 0, time.FixedZone("meter", 8*60*60))
	raw, err := EncodeReadLoadRecord("123456789012", "06020001", 12, &start)
	if err != nil { t.Fatal(err) }
	frame, err := DecodeResponse(raw, "123456789012")
	if err != nil { t.Fatal(err) }
	want := []byte{0x01, 0x00, 0x02, 0x06, 0x0c, 0x16, 0x13, 0x20, 0x11, 0x26}
	if !bytes.Equal(frame.Data, want) { t.Fatalf("payload=%x, want %x", frame.Data, want) }
	if raw[9] != 10 { t.Fatalf("data length=%d, want 10", raw[9]) }
}

func TestEncodeReadLoadRecordRejectsInvalidArguments(t *testing.T) {
	start := time.Date(2026, time.November, 20, 13, 16, 0, 0, time.UTC)
	cases := []struct{name, address, dataID string; blocks uint8; start *time.Time}{
		{"zero blocks", "123456789012", "06010000", 0, nil},
		{"bad address", "123", "06010000", 1, nil},
		{"bad DI", "123456789012", "xyz", 1, nil},
		{"ordinary measurement DI", "123456789012", "00010000", 1, nil},
		{"valid time-based DI", "123456789012", "06010001", 1, &start},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeReadLoadRecord(tc.address, tc.dataID, tc.blocks, tc.start)
			if tc.name == "valid time-based DI" {
				if err != nil { t.Fatal(err) }
				return
			}
			if err == nil { t.Fatal("expected invalid argument rejection") }
		})
	}
}
