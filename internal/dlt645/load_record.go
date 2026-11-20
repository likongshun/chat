package dlt645

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// EncodeReadLoadRecord builds a DL/T645-2007 0x11 load-profile request.
//
// With start == nil, the request asks for the newest blocks:
//   DI0..DI3, N
//
// With start != nil, the request asks for blocks beginning at the supplied
// meter-local timestamp:
//   DI0..DI3, N, mm, hh, DD, MM, YY
//
// The timestamp's wall-clock fields are used as meter-local time; no timezone
// conversion is performed. The caller must pass a time in the meter's clock
// domain. Data bytes are encoded by encodeFrame with the protocol's +0x33
// transform.
func EncodeReadLoadRecord(address, dataID string, blocks uint8, start *time.Time) ([]byte, error) {
	if blocks == 0 {
		return nil, fmt.Errorf("load-record block count must be 1..255: %w", ErrFrame)
	}
	addr, err := encodeAddress(address)
	if err != nil {
		return nil, err
	}
	id, err := decodeDataIdentifier(dataID)
	if err != nil {
		return nil, err
	}
	// Appendix A.7 load-record identifiers use DI3=06H, DI1=00H,
	// DI2=00H..06H (record class), and DI0=00H..02H (earliest,
	// given-time, latest). Reject ordinary measurement DIs here.
	if id[3] != 0x06 || id[1] != 0x00 || id[2] > 0x06 || id[0] > 0x02 {
		return nil, fmt.Errorf("data identifier %q is not a DL/T645-2007 Appendix A.7 load-record DI: %w", dataID, ErrFrame)
	}
	payload := make([]byte, 0, 10)
	payload = append(payload, id[:]...)
	payload = append(payload, blocks)
	if start != nil {
		if start.Year() < 2000 || start.Year() > 2099 {
			return nil, fmt.Errorf("load-record year must be in 2000..2099: %w", ErrFrame)
		}
		if start.Month() < time.January || start.Month() > time.December ||
			start.Day() < 1 || start.Day() > 31 ||
			start.Hour() < 0 || start.Hour() > 23 ||
			start.Minute() < 0 || start.Minute() > 59 {
			return nil, fmt.Errorf("invalid load-record start time: %w", ErrFrame)
		}
		calendar := time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute(), 0, 0, time.UTC)
		if calendar.Year() != start.Year() || calendar.Month() != start.Month() || calendar.Day() != start.Day() {
			return nil, fmt.Errorf("invalid load-record calendar date: %w", ErrFrame)
		}
		payload = append(payload,
			bcdByte(start.Minute()),
			bcdByte(start.Hour()),
			bcdByte(start.Day()),
			bcdByte(int(start.Month())),
			bcdByte(start.Year()%100),
		)
	}
	return encodeFrame(addr, ReadData, payload), nil
}

func decodeDataIdentifier(dataID string) ([4]byte, error) {
	var out [4]byte
	id, err := hex.DecodeString(strings.TrimSpace(dataID))
	if err != nil || len(id) != 4 {
		return out, fmt.Errorf("data identifier must be 8 hexadecimal digits: %w", ErrFrame)
	}
	for i := range id {
		out[i] = id[3-i]
	}
	return out, nil
}

func bcdByte(value int) byte {
	return byte((value/10)<<4 | value%10)
}
