package dlt645

import (
	"fmt"
	"sort"
	"strings"
)

// DataIDTemplate describes a commonly used DL/T645-2007 data identifier.
// Values are starting points only: manufacturers may vary supported IDs,
// lengths, sign conventions and decimal places.
type DataIDTemplate struct {
	ID string `json:"id"`
	Name string `json:"name"`
	Category string `json:"category"`
	DataType string `json:"data_type"`
	Length uint16 `json:"length"`
	Scale float64 `json:"scale"`
	Unit string `json:"unit"`
	Notes string `json:"notes"`
}

var standardDataIDTemplates = []DataIDTemplate{
	{ID:"00010000",Name:"正向有功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kWh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"00020000",Name:"反向有功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kWh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"00030000",Name:"正向无功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kvarh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"00040000",Name:"反向无功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kvarh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"02010100",Name:"A相电压",Category:"电压",DataType:"BCD",Length:2,Scale:0.1,Unit:"V",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02010200",Name:"B相电压",Category:"电压",DataType:"BCD",Length:2,Scale:0.1,Unit:"V",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02010300",Name:"C相电压",Category:"电压",DataType:"BCD",Length:2,Scale:0.1,Unit:"V",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02020100",Name:"A相电流",Category:"电流",DataType:"BCD",Length:3,Scale:0.001,Unit:"A",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02020200",Name:"B相电流",Category:"电流",DataType:"BCD",Length:3,Scale:0.001,Unit:"A",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02020300",Name:"C相电流",Category:"电流",DataType:"BCD",Length:3,Scale:0.001,Unit:"A",Notes:"常见标准数据项；请核对型号和小数位"},
}

// ListDataIDTemplates returns a copy sorted by category and identifier.
func ListDataIDTemplates() []DataIDTemplate {
	out := append([]DataIDTemplate(nil), standardDataIDTemplates...)
	sort.Slice(out, func(i,j int) bool {
		if out[i].Category == out[j].Category { return out[i].ID < out[j].ID }
		return out[i].Category < out[j].Category
	})
	return out
}

// FindDataIDTemplate performs a case-insensitive exact identifier lookup.
func FindDataIDTemplate(id string) (DataIDTemplate, bool) {
	id = strings.ToUpper(strings.TrimSpace(id))
	for _, item := range standardDataIDTemplates {
		if item.ID == id { return item, true }
	}
	return DataIDTemplate{}, false
}

// ValidateDataIDTemplate checks that a template can safely populate a read-only point.
func ValidateDataIDTemplate(item DataIDTemplate) error {
	if len(item.ID) != 8 { return fmt.Errorf("data id must contain 8 hexadecimal digits") }
	for _, r := range item.ID { if !strings.ContainsRune("0123456789ABCDEF", unicodeUpper(r)) { return fmt.Errorf("invalid data id %q", item.ID) } }
	if item.Length < 1 || item.Length > 4096 { return fmt.Errorf("data length must be 1..4096 bytes") }
	if item.DataType != "BCD" && item.DataType != "BCD_SIGNED" { return fmt.Errorf("unsupported template type %q", item.DataType) }
	return nil
}

func unicodeUpper(r rune) rune {
	if r >= 'a' && r <= 'f' { return r - ('a' - 'A') }
	return r
}
