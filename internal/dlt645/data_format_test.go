package dlt645

import (
	"reflect"
	"testing"
)

func TestDecodeDataFormatBCDAndDateTime(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		format string
		want any
	}{
		{"energy", []byte{0x78, 0x56, 0x34, 0x12}, "XXXXXX.XX", float64(123456.78)},
		{"clock", []byte{0x56, 0x34, 0x12}, "hhmmss", "123456"},
		{"date", []byte{0x02, 0x06, 0x11, 0x26}, "YYMMDDWW", "26110602"},
		{"ascii", []byte("METER  "), "ASCII:7", "METER"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeDataFormat(tt.data, tt.format)
			if err != nil { t.Fatal(err) }
			if !reflect.DeepEqual(got, tt.want) { t.Fatalf("got %#v want %#v", got, tt.want) }
		})
	}
}

func TestDecodeDataFormatRejectsMalformedValues(t *testing.T) {
	for _, tt := range []struct{ data []byte; format string }{
		{[]byte{0xFA}, "NN"},
		{[]byte{0x99, 0x99, 0x99}, "hhmmss"},
		{[]byte{0x31}, "UNKNOWN"},
		{[]byte{0x99, 0x99}, "ASCII:3"},
	} {
		if _, err := DecodeDataFormat(tt.data, tt.format); err == nil {
			t.Fatalf("expected error for %x / %s", tt.data, tt.format)
		}
	}
}
