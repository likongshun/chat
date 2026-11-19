package dlt645

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DecodeDataFormat decodes a DL/T645 data field after the frame layer has
// removed the 0x33 offset. The format syntax follows the common meter
// dictionaries: X/N are BCD digits, '.' is a decimal point, and date/time
// tokens are YYMMDDWW, YYMMDDhhmm, MMDDhhmm and hhmmss.
//
// Multi-byte BCD fields are little-endian on the wire. Date/time values are
// returned as strings to preserve the meter's calendar fields and avoid
// inventing a timezone.
func DecodeDataFormat(data []byte, format string) (any, error) {
	format = strings.TrimSpace(format)
	if len(data) == 0 || format == "" {
		return nil, fmt.Errorf("empty DL/T645 data or format")
	}
	switch strings.ToUpper(format) {
	case "YYMMDDWW":
		return decodeDateDigits(data, 4, format)
	case "YYMMDDHHMM":
		return decodeDateDigits(data, 5, format)
	case "MMDDHHMM":
		return decodeDateDigits(data, 4, format)
	case "HHMMSS":
		return decodeDateDigits(data, 3, format)
	}
	if strings.ContainsAny(format, "XxNn") {
		return decodeBCDFormat(data, format)
	}
	if strings.HasPrefix(strings.ToUpper(format), "ASCII:") {
		n, err := strconv.Atoi(format[len("ASCII:"):])
		if err != nil || n < 1 || n > len(data) {
			return nil, fmt.Errorf("invalid ASCII format %q", format)
		}
		return strings.TrimRight(string(data[:n]), "\x00 "), nil
	}
	return nil, fmt.Errorf("unsupported DL/T645 data format %q", format)
}

func decodeDateDigits(data []byte, byteCount int, format string) (string, error) {
	if len(data) != byteCount {
		return "", fmt.Errorf("format %s requires %d bytes, got %d", format, byteCount, len(data))
	}
	digits := make([]byte, 0, byteCount*2)
	for i := len(data)-1; i >= 0; i-- {
		hi, lo := data[i]>>4, data[i]&0x0f
		if hi > 9 || lo > 9 {
			return "", fmt.Errorf("invalid BCD byte 0x%02x", data[i])
		}
		digits = append(digits, '0'+hi, '0'+lo)
	}
	value := string(digits)
	switch strings.ToUpper(format) {
	case "YYMMDDWW":
		yy, _ := strconv.Atoi(value[0:2])
		month, _ := strconv.Atoi(value[2:4])
		day, _ := strconv.Atoi(value[4:6])
		weekday, _ := strconv.Atoi(value[6:8])
		if month < 1 || month > 12 || weekday > 7 || !validCalendarDate(2000+yy, month, day) {
			return "", fmt.Errorf("invalid DL/T645 date %s", value)
		}
	case "YYMMDDHHMM":
		yy, _ := strconv.Atoi(value[0:2])
		month, _ := strconv.Atoi(value[2:4])
		day, _ := strconv.Atoi(value[4:6])
		hour, _ := strconv.Atoi(value[6:8])
		minute, _ := strconv.Atoi(value[8:10])
		if !validCalendarDate(2000+yy, month, day) || hour > 23 || minute > 59 {
			return "", fmt.Errorf("invalid DL/T645 date/time %s", value)
		}
	case "MMDDHHMM":
		month, _ := strconv.Atoi(value[0:2])
		day, _ := strconv.Atoi(value[2:4])
		hour, _ := strconv.Atoi(value[4:6])
		minute, _ := strconv.Atoi(value[6:8])
		if month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 {
			return "", fmt.Errorf("invalid DL/T645 month/time %s", value)
		}
	case "HHMMSS":
		hour, _ := strconv.Atoi(value[0:2])
		minute, _ := strconv.Atoi(value[2:4])
		second, _ := strconv.Atoi(value[4:6])
		if hour > 23 || minute > 59 || second > 59 {
			return "", fmt.Errorf("invalid DL/T645 time %s", value)
		}
	}
	return value, nil
}

func validCalendarDate(year, month, day int) bool {
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return false
	}
	d := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return d.Year() == year && int(d.Month()) == month && d.Day() == day
}

func decodeBCDFormat(data []byte, format string) (float64, error) {
	digits := make([]byte, 0, len(data)*2)
	for i := len(data)-1; i >= 0; i-- {
		hi, lo := data[i]>>4, data[i]&0x0f
		if hi > 9 || lo > 9 {
			return 0, fmt.Errorf("invalid BCD byte 0x%02x", data[i])
		}
		digits = append(digits, '0'+hi, '0'+lo)
	}
	precision := 0
	if dot := strings.IndexByte(format, '.'); dot >= 0 {
		precision = len(format)-dot-1
	}
	for len(digits) < len(strings.ReplaceAll(format, ".", "")) {
		digits = append([]byte{'0'}, digits...)
	}
	if len(digits) > len(strings.ReplaceAll(format, ".", "")) {
		return 0, fmt.Errorf("BCD payload has %d digits, format %q allows %d", len(digits), format, len(strings.ReplaceAll(format, ".", "")))
	}
	n, err := strconv.ParseFloat(string(digits), 64)
	if err != nil {
		return 0, err
	}
	for i := 0; i < precision; i++ {
		n /= 10
	}
	return n, nil
}
