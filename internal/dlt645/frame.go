package dlt645

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	FrameStart byte = 0x68
	FrameEnd   byte = 0x16
	ReadData   byte = 0x11
)

var (
	ErrFrame = errors.New("invalid DL/T645-2007 frame")
	ErrChecksum = errors.New("DL/T645-2007 checksum mismatch")
)

// Frame is a decoded DL/T645-2007 frame. Address is the canonical 12-digit
// meter address; Data contains the wire payload after removing the 0x33 offset.
type Frame struct {
	Address string
	Control byte
	Data    []byte
}

// EncodeReadData builds a master read-data request. Address must contain
// exactly 12 decimal digits. dataID is the conventional DI3..DI0 hexadecimal notation; the request
// transmits the four identifier bytes in DL/T645 wire order DI0..DI3.
func EncodeReadData(address, dataID string) ([]byte, error) {
	addr, err := encodeAddress(address)
	if err != nil { return nil, err }
	id, err := hex.DecodeString(strings.TrimSpace(dataID))
	if err != nil || len(id) != 4 { return nil, fmt.Errorf("data identifier must be 8 hexadecimal digits: %w", ErrFrame) }
	payload := make([]byte, 4)
	for i := range id { payload[i] = id[3-i] + 0x33 }
	return encodeFrame(addr, ReadData, payload), nil
}

// DecodeResponse validates framing, address, length and checksum, then
// removes the DL/T645 data-byte 0x33 offset.
func DecodeResponse(raw []byte, expectedAddress string) (Frame, error) {
	start := -1
	for i, b := range raw { if b == FrameStart { start = i; break } }
	if start < 0 || len(raw)-start < 12 { return Frame{}, ErrFrame }
	raw = raw[start:]
	if raw[0] != FrameStart || raw[7] != FrameStart { return Frame{}, ErrFrame }
	n := int(raw[9])
	if n > 200 || len(raw) != 12+n || raw[len(raw)-1] != FrameEnd { return Frame{}, ErrFrame }
	var sum byte
	for _, b := range raw[:len(raw)-2] { sum += b }
	if sum != raw[len(raw)-2] { return Frame{}, ErrChecksum }
	addr, err := decodeAddress(raw[1:7])
	if err != nil { return Frame{}, err }
	if expectedAddress != "" && addr != expectedAddress { return Frame{}, fmt.Errorf("response address %s does not match %s", addr, expectedAddress) }
	data := append([]byte(nil), raw[10:10+n]...)
	for i := range data { data[i] -= 0x33 }
	return Frame{Address:addr, Control:raw[8], Data:data}, nil
}

func encodeFrame(addr [6]byte, control byte, data []byte) []byte {
	out := make([]byte, 0, 12+len(data))
	out = append(out, FrameStart)
	out = append(out, addr[:]...)
	out = append(out, FrameStart, control, byte(len(data)))
	out = append(out, data...)
	var sum byte
	for _, b := range out { sum += b }
	out = append(out, sum, FrameEnd)
	return out
}

func encodeAddress(s string) ([6]byte, error) {
	var out [6]byte
	s = strings.TrimSpace(s)
	if len(s) != 12 { return out, fmt.Errorf("meter address must be 12 digits: %w", ErrFrame) }
	for i := 0; i < 6; i++ {
		hi, lo := s[10-2*i], s[11-2*i]
		if hi < '0' || hi > '9' || lo < '0' || lo > '9' { return out, ErrFrame }
		out[i] = (hi-'0')<<4 | (lo-'0')
	}
	return out, nil
}

func decodeAddress(b []byte) (string, error) {
	if len(b) != 6 { return "", ErrFrame }
	var out strings.Builder
	for i:=5; i>=0; i-- {
		if b[i]>>4 > 9 || b[i]&0x0f > 9 { return "", ErrFrame }
		out.WriteByte('0' + (b[i] >> 4))
		out.WriteByte('0' + (b[i] & 0x0f))
	}
	return out.String(), nil
}
