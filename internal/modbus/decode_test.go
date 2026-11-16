package modbus

import (
	"math"
	"testing"

	"github.com/likongshun/chat/internal/config"
)

func TestDecodeFloat32(t *testing.T) {
	p:=config.Point{PointCode:"temperature",Function:3,Count:2,DataType:"FLOAT32",Scale:1,ByteOrder:"ABCD"}
	got,err:=DecodeRegisters(p,[]uint16{0x41c8,0x0000})
	if err!=nil {t.Fatal(err)}
	if math.Abs(got.(float64)-25)>0.0001 {t.Fatalf("expected 25, got %v",got)}
}
func TestDecodeFloat32WordSwap(t *testing.T) {
	p:=config.Point{Function:3,Count:2,DataType:"FLOAT32",Scale:1,ByteOrder:"CDAB"}
	got,err:=DecodeRegisters(p,[]uint16{0x0000,0x41c8})
	if err!=nil {t.Fatal(err)}
	if math.Abs(got.(float64)-25)>0.0001 {t.Fatalf("expected 25, got %v",got)}
}
func TestDecodeBit(t *testing.T) {
	p:=config.Point{PointCode:"running",Function:1,Count:1,DataType:"BOOL"}
	got,err:=DecodeBit(p,[]bool{true})
	if err!=nil || !got {t.Fatalf("got=%v err=%v",got,err)}
}
func TestDecodeString(t *testing.T) {
	p:=config.Point{Function:3,Count:2,DataType:"STRING"}
	got,err:=DecodeRegisters(p,[]uint16{0x4142,0x4300})
	if err!=nil || got!="ABC" {t.Fatalf("got=%v err=%v",got,err)}
}
