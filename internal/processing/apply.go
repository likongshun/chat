package processing

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/likongshun/chat/internal/config"
)

// Apply executes the prevalidated operators configured for one point.
// It never evaluates arbitrary scripts or expressions.
func Apply(pointCode string, value any, rules []config.Processing, enums []config.EnumValue) (any, error) {
	current := value
	for _, rule := range rules {
		if rule.PointCode != pointCode {
			continue
		}
		op := strings.ToUpper(strings.TrimSpace(rule.Operation))
		switch op {
		case "ENUM":
			key := fmt.Sprint(current)
			found := false
			for _, item := range enums {
				if item.PointCode == pointCode && item.Value == key {
					current = item.Label
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("point %s: no enum label for value %q", pointCode, key)
			}
		case "SCALE", "LINEAR":
			n, err := number(current, pointCode, op)
			if err != nil {
				return nil, err
			}
			current = n*rule.Param1 + rule.Param2
		case "ABS":
			n, err := number(current, pointCode, op)
			if err != nil {
				return nil, err
			}
			current = math.Abs(n)
		case "CLAMP":
			n, err := number(current, pointCode, op)
			if err != nil {
				return nil, err
			}
			current = math.Max(rule.Param1, math.Min(rule.Param2, n))
		case "BIT":
			n, err := number(current, pointCode, op)
			if err != nil {
				return nil, err
			}
			bit := uint(rule.Param1)
			current = uint64(n)&(uint64(1)<<bit) != 0
		case "ROUND":
			n, err := number(current, pointCode, op)
			if err != nil {
				return nil, err
			}
			factor := math.Pow10(int(rule.Param1))
			current = math.Round(n*factor) / factor
		case "DIVIDE":
			n, err := number(current, pointCode, op)
			if err != nil {
				return nil, err
			}
			if rule.Param1 == 0 {
				return nil, fmt.Errorf("point %s: DIVIDE divisor cannot be zero", pointCode)
			}
			current = n / rule.Param1
		default:
			return nil, fmt.Errorf("point %s: unsupported operation %q", pointCode, rule.Operation)
		}
	}
	return current, nil
}

func number(value any, pointCode, operation string) (float64, error) {
	var n float64
	switch v := value.(type) {
	case float64:
		n = v
	case float32:
		n = float64(v)
	case int:
		n = float64(v)
	case int64:
		n = float64(v)
	case uint64:
		n = float64(v)
	case bool:
		if v {
			n = 1
		}
	default:
		return 0, fmt.Errorf("point %s: %s requires numeric input, got %T", pointCode, operation, value)
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("point %s: %s received non-finite input", pointCode, operation)
	}
	return n, nil
}

func EnumKey(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
