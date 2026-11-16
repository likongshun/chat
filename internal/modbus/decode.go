package modbus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/likongshun/chat/internal/config"
)

func DecodeRegisters(point config.Point, registers []uint16) (any, error) {
	if point.Function < 3 || point.Function > 4 {
		return nil, fmt.Errorf("point %s: function %d is not a register function", point.PointCode, point.Function)
	}
	if len(registers) != int(point.Count) {
		return nil, fmt.Errorf("point %s: expected %d registers, got %d", point.PointCode, point.Count, len(registers))
	}
	raw := make([]byte, len(registers)*2)
	for i, r := range registers { binary.BigEndian.PutUint16(raw[i*2:], r) }

	switch strings.ToUpper(point.DataType) {
	case "STRING":
		s := strings.TrimRight(string(raw), "\x00 ")
		return s, nil
	case "INT16":
		if len(raw)!=2 { return nil, errors.New("INT16 requires one register") }
		return float64(int16(binary.BigEndian.Uint16(apply16Order(raw, point.ByteOrder))))*point.Scale + point.Offset, nil
	case "UINT16":
		if len(raw)!=2 { return nil, errors.New("UINT16 requires one register") }
		return float64(binary.BigEndian.Uint16(apply16Order(raw, point.ByteOrder)))*point.Scale + point.Offset, nil
	case "INT32", "UINT32", "FLOAT32":
		if len(raw)!=4 { return nil, fmt.Errorf("%s requires two registers", point.DataType) }
		b := apply32Order(raw, point.ByteOrder)
		if strings.EqualFold(point.WordOrder,"LOW_HIGH") { swapWords(b) }
		v := binary.BigEndian.Uint32(b)
		switch strings.ToUpper(point.DataType) {
		case "INT32": return float64(int32(v))*point.Scale + point.Offset, nil
		case "UINT32": return float64(v)*point.Scale + point.Offset, nil
		default: return float64(math.Float32frombits(v))*point.Scale + point.Offset, nil
		}
	case "INT64", "UINT64", "FLOAT64":
		if len(raw)!=8 { return nil, fmt.Errorf("%s requires four registers", point.DataType) }
		b := make([]byte,8)
		copy(b,raw)
		for i:=0;i<4;i++ { word := strings.ToUpper(point.ByteOrder); _ = word }
		if strings.EqualFold(point.WordOrder,"LOW_HIGH") { reverseWords(b) }
		v := binary.BigEndian.Uint64(b)
		switch strings.ToUpper(point.DataType) {
		case "INT64": return float64(int64(v))*point.Scale + point.Offset, nil
		case "UINT64": return float64(v)*point.Scale + point.Offset, nil
		default: return math.Float64frombits(v)*point.Scale + point.Offset, nil
		}
	default:
		return nil, fmt.Errorf("unsupported register data type %q", point.DataType)
	}
}

func DecodeBit(point config.Point, bits []bool) (bool, error) {
	if point.Function != 1 && point.Function != 2 {
		return false, fmt.Errorf("point %s: function %d is not a bit function", point.PointCode, point.Function)
	}
	if point.DataType != "BOOL" { return false, fmt.Errorf("point %s: bit point must use BOOL", point.PointCode) }
	if point.Count != 1 || len(bits) != 1 { return false, errors.New("BOOL point must map to exactly one bit") }
	return bits[0], nil
}

func apply16Order(in []byte, order string) []byte {
	out:=append([]byte(nil),in...)
	switch strings.ToUpper(order) {
	case "BADC", "DCBA": out[0],out[1]=out[1],out[0]
	}
	return out
}
func apply32Order(in []byte, order string) []byte {
	out:=append([]byte(nil),in...)
	switch strings.ToUpper(order) {
	case "BADC": out[0],out[1],out[2],out[3]=in[1],in[0],in[3],in[2]
	case "CDAB": out[0],out[1],out[2],out[3]=in[2],in[3],in[0],in[1]
	case "DCBA": out[0],out[1],out[2],out[3]=in[3],in[2],in[1],in[0]
	}
	return out
}
func swapWords(b []byte) { for i:=0;i+3<len(b);i+=4 { b[i],b[i+1],b[i+2],b[i+3]=b[i+2],b[i+3],b[i],b[i+1] } }
func reverseWords(b []byte) {
	for i,j:=0,len(b)-2;i<j;i,j=i+2,j-2 { b[i],b[i+1],b[j],b[j+1]=b[j],b[j+1],b[i],b[i+1] }
}
