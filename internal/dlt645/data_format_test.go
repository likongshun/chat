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


func TestValidateDataFormatLength(t *testing.T) {
	for _, tt := range []struct{ format string; length uint16; wantErr bool }{
		{"XXXXXX.XX",4,false}, {"XXX.X",2,false}, {"YYMMDDWW",4,false},
		{"YYMMDDHHMM",5,false}, {"HHMMSS",3,false}, {"XXX.XXX+YYMMDDhhmm",8,false},
		{"XXXXXX.XX",3,true}, {"UNKNOWN",1,true}, {"ASCII:3",2,true},
	} {
		err := ValidateDataFormat(tt.format, tt.length)
		if (err != nil) != tt.wantErr { t.Errorf("ValidateDataFormat(%q,%d) error=%v",tt.format,tt.length,err) }
	}
}

func TestDecodeCompositeMaximumDemand(t *testing.T) {
	got, err := DecodeDataFormat([]byte{0x56,0x34,0x12,0x30,0x12,0x19,0x11,0x26}, "XXX.XXX+YYMMDDhhmm")
	if err != nil { t.Fatal(err) }
	value := got.(map[string]any)
	if value["value"] != float64(123.456) { t.Fatalf("unexpected value: %#v",value["value"]) }
	if value["occurred_at"] != "2611191230" { t.Fatalf("unexpected timestamp: %#v",value["occurred_at"]) }
}
