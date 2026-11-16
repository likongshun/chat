package processing

import (
 "encoding/binary"
 "math"
)
func Float32(regs []uint16,order string)(float64,bool){
 if len(regs)<2{return 0,false};a,b:=regs[0],regs[1]
 switch order{case "CDAB":a,b=b,a;case "BADC":a=(a<<8)|(a>>8);b=(b<<8)|(b>>8);case "DCBA":a,b=b,a;a=(a<<8)|(a>>8);b=(b<<8)|(b>>8)}
 raw:=uint32(a)<<16|uint32(b);return float64(math.Float32frombits(raw)),true
}
func Int32(regs []uint16)(int32,bool){if len(regs)<2{return 0,false};return int32(binary.BigEndian.Uint32([]byte{byte(regs[0]>>8),byte(regs[0]),byte(regs[1]>>8),byte(regs[1])})),true}
func Apply(value,scale,offset float64)float64{return value*scale+offset}
