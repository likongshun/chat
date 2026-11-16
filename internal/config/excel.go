package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// LoadXLSX loads the five-sheet configuration workbook. Columns are resolved
// by header name, not by position, so harmless column reordering is supported.
func LoadXLSX(path string) (Config, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("open workbook: %w", err)
	}
	defer f.Close()

	var c Config
	if c.Devices, err = readDevices(f); err != nil { return Config{}, err }
	if c.Points, err = readPoints(f); err != nil { return Config{}, err }
	if c.Processing, err = readProcessing(f); err != nil { return Config{}, err }
	if c.WriteRules, err = readWriteRules(f); err != nil { return Config{}, err }
	if c.Enums, err = readEnums(f); err != nil { return Config{}, err }
	if err = c.Validate(); err != nil { return Config{}, fmt.Errorf("configuration rejected: %w", err) }
	return c, nil
}

func sheetRows(f *excelize.File, sheet string, required ...string) ([][]string, map[string]int, error) {
	rows, err := f.GetRows(sheet)
	if err != nil { return nil, nil, fmt.Errorf("sheet %s: %w", sheet, err) }
	if len(rows) == 0 { return nil, nil, fmt.Errorf("sheet %s is empty", sheet) }
	cols := make(map[string]int, len(rows[0]))
	for i, h := range rows[0] {
		name := strings.ToLower(strings.TrimSpace(h))
		if name == "" { continue }
		if _, exists := cols[name]; exists { return nil, nil, fmt.Errorf("sheet %s: duplicate column %q", sheet, name) }
		cols[name] = i
	}
	for _, name := range required {
		if _, ok := cols[name]; !ok { return nil, nil, fmt.Errorf("sheet %s: missing column %q", sheet, name) }
	}
	return rows[1:], cols, nil
}
func value(row []string, cols map[string]int, name string) string {
	i, ok := cols[name]
	if !ok || i >= len(row) { return "" }
	return strings.TrimSpace(row[i])
}
func parseInt(row []string, cols map[string]int, name string, def int64) (int64, error) {
	s := value(row, cols, name)
	if s == "" { return def, nil }
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil { return 0, fmt.Errorf("%s=%q must be an integer: %w", name, s, err) }
	return n, nil
}
func parseFloat(row []string, cols map[string]int, name string, def float64) (float64, error) {
	s := value(row, cols, name)
	if s == "" { return def, nil }
	n, err := strconv.ParseFloat(s, 64)
	if err != nil { return 0, fmt.Errorf("%s=%q: %w", name, s, err) }
	return n, nil
}
func parseBool(row []string, cols map[string]int, name string, def bool) (bool, error) {
	s := strings.ToUpper(value(row, cols, name))
	if s == "" { return def, nil }
	switch s {
	case "TRUE", "1", "YES": return true, nil
	case "FALSE", "0", "NO": return false, nil
	default: return false, fmt.Errorf("%s=%q must be TRUE/FALSE", name, s)
	}
}
func rowErr(sheet string, n int, err error) error { return fmt.Errorf("%s row %d: %w", sheet, n+2, err) }

func readDevices(f *excelize.File) ([]Device, error) {
	rows, cols, err := sheetRows(f, "Devices", "device_id", "host", "port", "unit_id", "model", "poll_ms", "enabled")
	if err != nil { return nil, err }
	out := make([]Device, 0, len(rows))
	for i, r := range rows {
		if value(r, cols, "device_id") == "" { continue }
		port, e := parseInt(r, cols, "port", 502); if e != nil { return nil, rowErr("Devices", i, e) }
		unit, e := parseInt(r, cols, "unit_id", 1); if e != nil { return nil, rowErr("Devices", i, e) }
		poll, e := parseInt(r, cols, "poll_ms", 1000); if e != nil { return nil, rowErr("Devices", i, e) }
		enabled, e := parseBool(r, cols, "enabled", true); if e != nil { return nil, rowErr("Devices", i, e) }
		if port < 1 || port > 65535 { return nil, rowErr("Devices", i, fmt.Errorf("port out of range: %d", port)) }
		if unit < 0 || unit > 255 { return nil, rowErr("Devices", i, fmt.Errorf("unit_id out of range: %d", unit)) }
		if poll < 100 || poll > int64((1<<63-1)/int64(time.Millisecond)) { return nil, rowErr("Devices", i, fmt.Errorf("poll_ms out of range: %d", poll)) }
		out = append(out, Device{
			ID: value(r, cols, "device_id"), Name: value(r, cols, "name"),
			Host: value(r, cols, "host"), Port: uint16(port), UnitID: byte(unit),
			Model: value(r, cols, "model"), Interval: time.Duration(poll)*time.Millisecond, Enabled: enabled,
		})
	}
	return out, nil
}
func readPoints(f *excelize.File) ([]Point, error) {
	rows, cols, err := sheetRows(f, "Points", "model", "point_code", "function", "address", "type", "length", "scale", "offset", "rw", "enabled")
	if err != nil { return nil, err }
	out := make([]Point, 0, len(rows))
	for i, r := range rows {
		model, code := value(r, cols, "model"), value(r, cols, "point_code")
		if model == "" && code == "" { continue }
		fn, e := parseInt(r, cols, "function", 3); if e != nil { return nil, rowErr("Points", i, e) }
		addr, e := parseInt(r, cols, "address", 0); if e != nil { return nil, rowErr("Points", i, e) }
		length, e := parseInt(r, cols, "length", 0); if e != nil { return nil, rowErr("Points", i, e) }
		scale, e := parseFloat(r, cols, "scale", 1); if e != nil { return nil, rowErr("Points", i, e) }
		offset, e := parseFloat(r, cols, "offset", 0); if e != nil { return nil, rowErr("Points", i, e) }
		enabled, e := parseBool(r, cols, "enabled", true); if e != nil { return nil, rowErr("Points", i, e) }
		rw := strings.ToUpper(value(r, cols, "rw"))
		if rw == "" { rw = "R" }
		if rw != "R" && rw != "RW" { return nil, rowErr("Points", i, fmt.Errorf("rw must be R or RW")) }
		if fn < 1 || fn > 4 { return nil, rowErr("Points", i, fmt.Errorf("function must be 1..4")) }
		if addr < 0 || addr > 65535 { return nil, rowErr("Points", i, fmt.Errorf("address out of range: %d", addr)) }
		if length < 0 || length > 2000 { return nil, rowErr("Points", i, fmt.Errorf("length out of range: %d", length)) }
		dtype := strings.ToUpper(value(r, cols, "type"))
		count := length
		if count == 0 { count = int64(registerWidths[dtype]) }
		if dtype == "STRING" && count == 0 { count = length }
		if count < 1 || count > 2000 { return nil, rowErr("Points", i, fmt.Errorf("invalid length/count: %d", count)) }
		if fn >= 3 && count > 125 { return nil, rowErr("Points", i, fmt.Errorf("register read length exceeds 125")) }
		if uint64(addr)+uint64(count) > 65536 { return nil, rowErr("Points", i, fmt.Errorf("address range exceeds 65535")) }
		out = append(out, Point{
			ID: model + "/" + code, Model: model, PointCode: code, Name: value(r, cols, "name"),
			Address: uint16(addr), Count: uint16(count), Function: byte(fn), DataType: dtype,
			Scale: scale, Offset: offset, ByteOrder: strings.ToUpper(value(r, cols, "byte_order")),
			WordOrder: strings.ToUpper(value(r, cols, "word_order")), Unit: value(r, cols, "unit"),
			ReadGroup: value(r, cols, "read_group"), Enabled: enabled, Writable: rw == "RW",
		})
	}
	return out, nil
}
func readProcessing(f *excelize.File) ([]Processing, error) {
	rows, cols, err := sheetRows(f, "Processing", "point_code", "operation", "param1", "param2")
	if err != nil { return nil, err }
	out := make([]Processing, 0, len(rows))
	for i, r := range rows {
		if value(r, cols, "point_code") == "" { continue }
		a, e := parseFloat(r, cols, "param1", 0); if e != nil { return nil, rowErr("Processing", i, e) }
		b, e := parseFloat(r, cols, "param2", 0); if e != nil { return nil, rowErr("Processing", i, e) }
		out = append(out, Processing{PointCode: value(r, cols, "point_code"), Operation: strings.ToUpper(value(r, cols, "operation")), Param1: a, Param2: b})
	}
	return out, nil
}
func readWriteRules(f *excelize.File) ([]WriteRule, error) {
	rows, cols, err := sheetRows(f, "WriteRules", "model", "point_code", "enabled", "min", "max", "require_confirm")
	if err != nil { return nil, err }
	out := make([]WriteRule, 0, len(rows))
	for i, r := range rows {
		if value(r, cols, "model") == "" && value(r, cols, "point_code") == "" { continue }
		enabled, e := parseBool(r, cols, "enabled", false); if e != nil { return nil, rowErr("WriteRules", i, e) }
		min, e := parseFloat(r, cols, "min", 0); if e != nil { return nil, rowErr("WriteRules", i, e) }
		max, e := parseFloat(r, cols, "max", 0); if e != nil { return nil, rowErr("WriteRules", i, e) }
		confirm, e := parseBool(r, cols, "require_confirm", true); if e != nil { return nil, rowErr("WriteRules", i, e) }
		out = append(out, WriteRule{Model: value(r, cols, "model"), PointCode: value(r, cols, "point_code"), Enabled: enabled, Min: min, Max: max, RequireConfirm: confirm})
	}
	return out, nil
}
func readEnums(f *excelize.File) ([]EnumValue, error) {
	rows, cols, err := sheetRows(f, "Enums", "point_code", "value", "label")
	if err != nil { return nil, err }
	out := make([]EnumValue, 0, len(rows))
	for _, r := range rows {
		if value(r, cols, "point_code") == "" { continue }
		out = append(out, EnumValue{PointCode: value(r, cols, "point_code"), Value: value(r, cols, "value"), Label: value(r, cols, "label")})
	}
	return out, nil
}
