package config

import (
	"fmt"
	"math"
	"time"
)

type Device struct {
	ID       string
	Name     string
	Host     string
	Port     uint16
	UnitID   byte
	Model    string
	Interval time.Duration
	Enabled  bool
}

type Point struct {
	ID        string
	DeviceID  string
	Model     string
	PointCode string
	Name      string
	Address   uint16
	Count     uint16
	Function  byte
	DataType  string
	Scale     float64
	Offset    float64
	ByteOrder string
	WordOrder string
	Unit      string
	ReadGroup string
	Enabled   bool
	Writable  bool
}

type Processing struct {
	PointCode string
	Operation string
	Param1    float64
	Param2    float64
}

type WriteRule struct {
	Model          string
	PointCode      string
	Enabled        bool
	Min            float64
	Max            float64
	RequireConfirm bool
}

type EnumValue struct {
	PointCode string
	Value     string
	Label     string
}

type Config struct {
	Devices    []Device
	Points     []Point
	Processing []Processing
	WriteRules []WriteRule
	Enums      []EnumValue
}

var registerWidths = map[string]uint16{
	"INT16": 1, "UINT16": 1, "BOOL": 1,
	"INT32": 2, "UINT32": 2, "FLOAT32": 2,
	"INT64": 4, "UINT64": 4, "FLOAT64": 4,
}

var allowedOperations = map[string]bool{
	"SCALE": true, "ABS": true, "CLAMP": true, "BIT": true,
	"ENUM": true, "ROUND": true, "DIVIDE": true, "LINEAR": true,
}

// Validate rejects unsafe or ambiguous configurations. This release has no
// Modbus write execution path: writable points and enabled write rules fail closed.
func (c Config) Validate() error {
	devices := make(map[string]Device, len(c.Devices))
	for _, d := range c.Devices {
		if d.ID == "" || d.Host == "" || d.Model == "" {
			return fmt.Errorf("device %q: id, host and model are required", d.ID)
		}
		if _, ok := devices[d.ID]; ok {
			return fmt.Errorf("duplicate device id %q", d.ID)
		}
		if d.Port == 0 || d.Interval < 100*time.Millisecond {
			return fmt.Errorf("device %s: port must be set and interval must be >=100ms", d.ID)
		}
		devices[d.ID] = d
	}

	pointKeys := make(map[string]bool, len(c.Points))
	pointCodes := make(map[string]bool, len(c.Points))
	for _, p := range c.Points {
		key := p.Model + "/" + p.PointCode
		if p.Model == "" {
			key = p.DeviceID + "/" + p.ID
		}
		if p.PointCode == "" && p.ID == "" {
			return fmt.Errorf("point: point_code or id is required")
		}
		if pointKeys[key] {
			return fmt.Errorf("duplicate point %q", key)
		}
		pointKeys[key] = true
		if p.Model == "" {
			if _, ok := devices[p.DeviceID]; !ok {
				return fmt.Errorf("point %s references unknown device %s", key, p.DeviceID)
			}
		}
		if p.Count == 0 {
			return fmt.Errorf("point %s: count must be positive", key)
		}
		if p.Function < 1 || p.Function > 4 {
			return fmt.Errorf("point %s: read function must be FC01..FC04", key)
		}
		if (p.Function == 1 || p.Function == 2) && p.Count > 2000 {
			return fmt.Errorf("point %s: bit count exceeds protocol limit", key)
		}
		if (p.Function == 3 || p.Function == 4) && p.Count > 125 {
			return fmt.Errorf("point %s: register count exceeds protocol limit", key)
		}
		if uint32(p.Address)+uint32(p.Count) > 65536 {
			return fmt.Errorf("point %s: address range exceeds Modbus address space", key)
		}
		if p.Writable {
			return fmt.Errorf("point %s: write execution is disabled", key)
		}
		width, ok := registerWidths[p.DataType]
		if !ok {
			if p.DataType == "STRING" {
				if p.Count < 1 || p.Count > 120 {
					return fmt.Errorf("point %s: STRING count must be 1..120 registers", key)
				}
			} else {
				return fmt.Errorf("point %s: unsupported data type %q", key, p.DataType)
			}
		} else if p.Function >= 3 && p.Count != width {
			return fmt.Errorf("point %s: %s requires %d registers, got %d", key, p.DataType, width, p.Count)
		}
		if p.Function <= 2 && (p.DataType != "BOOL" || p.Count != 1) {
			return fmt.Errorf("point %s: FC01/FC02 points must map exactly one BOOL bit", key)
		}
		if p.Function >= 3 && p.DataType == "BOOL" {
			return fmt.Errorf("point %s: BOOL must use FC01/FC02", key)
		}
		if !finite(p.Scale) || !finite(p.Offset) {
			return fmt.Errorf("point %s: scale and offset must be finite", key)
		}
		if p.ByteOrder != "" && p.ByteOrder != "ABCD" && p.ByteOrder != "BADC" && p.ByteOrder != "CDAB" && p.ByteOrder != "DCBA" {
			return fmt.Errorf("point %s: invalid byte order %q", key, p.ByteOrder)
		}
		if p.WordOrder != "" && p.WordOrder != "HIGH_LOW" && p.WordOrder != "LOW_HIGH" {
			return fmt.Errorf("point %s: invalid word order %q", key, p.WordOrder)
		}
		if p.PointCode != "" {
			pointCodes[p.PointCode] = true
		}
	}

	for _, d := range c.Devices {
		if !d.Enabled {
			continue
		}
		found := false
		for _, p := range c.Points {
			if p.Enabled && p.Model == d.Model {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("device %s: model %s has no enabled points", d.ID, d.Model)
		}
	}

	for _, p := range c.Processing {
		if !allowedOperations[p.Operation] {
			return fmt.Errorf("point %s: unsupported processing operation %q", p.PointCode, p.Operation)
		}
		if !pointCodes[p.PointCode] {
			return fmt.Errorf("processing references unknown point_code %q", p.PointCode)
		}
		if !finite(p.Param1) || !finite(p.Param2) {
			return fmt.Errorf("point %s: processing parameters must be finite", p.PointCode)
		}
		switch p.Operation {
		case "BIT":
			if p.Param1 < 0 || p.Param1 > 63 || math.Trunc(p.Param1) != p.Param1 {
				return fmt.Errorf("point %s: BIT index must be an integer in 0..63", p.PointCode)
			}
		case "CLAMP":
			if p.Param1 > p.Param2 {
				return fmt.Errorf("point %s: CLAMP minimum exceeds maximum", p.PointCode)
			}
		case "ROUND":
			if p.Param1 < -9 || p.Param1 > 9 || math.Trunc(p.Param1) != p.Param1 {
				return fmt.Errorf("point %s: ROUND precision must be an integer in -9..9", p.PointCode)
			}
		case "DIVIDE":
			if p.Param1 == 0 {
				return fmt.Errorf("point %s: DIVIDE divisor cannot be zero", p.PointCode)
			}
		}
	}
	for _, w := range c.WriteRules {
		if w.Enabled {
			return fmt.Errorf("write rule %s/%s is enabled; write path is disabled", w.Model, w.PointCode)
		}
		if w.Model == "" || w.PointCode == "" || !finite(w.Min) || !finite(w.Max) || w.Min > w.Max {
			return fmt.Errorf("invalid write rule %s/%s", w.Model, w.PointCode)
		}
	}
	enumKeys := make(map[string]bool)
	for _, e := range c.Enums {
		key := e.PointCode + "/" + e.Value
		if e.PointCode == "" || e.Label == "" {
			return fmt.Errorf("invalid enum mapping %q", key)
		}
		if enumKeys[key] {
			return fmt.Errorf("duplicate enum mapping %q", key)
		}
		enumKeys[key] = true
	}
	return nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
