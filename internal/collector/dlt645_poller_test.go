package collector

import (
	"math"
	"testing"
)

func TestDecode645ValueBCDAndScale(t *testing.T) {
	// DI 00 01 00 00 followed by BCD 34 12 (wire low byte first => 1234).
	got,err:=decode645Value([]byte{0x00,0x01,0x00,0x00,0x34,0x12},"00010000",0.01,0)
	if err!=nil { t.Fatal(err) }
	if math.Abs(got-12.34)>1e-9 { t.Fatalf("got %v want 12.34",got) }
}
func TestDecode645ValueRejectsBadBCDAndIdentifier(t *testing.T) {
	if _,err:=decode645Value([]byte{0,1,0,0,0xfa},"00010000",1,0); err==nil { t.Fatal("expected invalid BCD") }
	if _,err:=decode645Value([]byte{0,1,0,0,1},"00020000",1,0); err==nil { t.Fatal("expected data identifier mismatch") }
}
