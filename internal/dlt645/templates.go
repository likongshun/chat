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
	DataFormat string `json:"data_format"`
	Length uint16 `json:"length"`
	Scale float64 `json:"scale"`
	Unit string `json:"unit"`
	Notes string `json:"notes"`
}

var standardDataIDTemplates = []DataIDTemplate{
	{ID:"00000000",Name:"组合有功总电能",Category:"电能",DataType:"BCD",DataFormat:"XXXXXX.XX",Length:4,Scale:1,Unit:"kWh",Notes:"数据标识与倍率应以电表型号说明书为准"},
	{ID:"00010000",Name:"正向有功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kWh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"00020000",Name:"反向有功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kWh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"00030000",Name:"正向无功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kvarh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"00040000",Name:"反向无功总电能",Category:"电能",DataType:"BCD",Length:4,Scale:0.01,Unit:"kvarh",Notes:"常见标准数据项；以电表说明书的精度和数据长度为准"},
	{ID:"02010100",Name:"A相电压",Category:"电压",DataType:"BCD",DataFormat:"XXX.X",Length:2,Scale:1,Unit:"V",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02010200",Name:"B相电压",Category:"电压",DataType:"BCD",DataFormat:"XXX.X",Length:2,Scale:1,Unit:"V",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02010300",Name:"C相电压",Category:"电压",DataType:"BCD",DataFormat:"XXX.X",Length:2,Scale:1,Unit:"V",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02020100",Name:"A相电流",Category:"电流",DataType:"BCD",DataFormat:"XX.XXX",Length:3,Scale:1,Unit:"A",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02020200",Name:"B相电流",Category:"电流",DataType:"BCD",DataFormat:"XX.XXX",Length:3,Scale:1,Unit:"A",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02020300",Name:"C相电流",Category:"电流",DataType:"BCD",DataFormat:"XX.XXX",Length:3,Scale:1,Unit:"A",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"01010000",Name:"正向有功总最大需量及发生时间",Category:"最大需量",DataType:"BCD",DataFormat:"XXX.XXX+YYMMDDhhmm",Length:8,Scale:1,Unit:"kW",Notes:"复合字段：需量值+发生时间；需按专用复合解码器处理"},
	{ID:"01020000",Name:"反向有功总最大需量及发生时间",Category:"最大需量",DataType:"BCD",DataFormat:"XXX.XXX+YYMMDDhhmm",Length:8,Scale:1,Unit:"kW",Notes:"复合字段：需量值+发生时间；需按专用复合解码器处理"},
	{ID:"02030100",Name:"总有功功率",Category:"功率",DataType:"BCD",DataFormat:"XXX.XXX",Length:3,Scale:1,Unit:"kW",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02050100",Name:"总无功功率",Category:"功率",DataType:"BCD",DataFormat:"XXX.XXX",Length:3,Scale:1,Unit:"kvar",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02040100",Name:"总功率因数",Category:"功率因数",DataType:"BCD",DataFormat:"X.XXX",Length:2,Scale:1,Unit:"",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02060100",Name:"A相功率因数",Category:"功率因数",DataType:"BCD",DataFormat:"X.XXX",Length:2,Scale:1,Unit:"",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02060200",Name:"B相功率因数",Category:"功率因数",DataType:"BCD",DataFormat:"X.XXX",Length:2,Scale:1,Unit:"",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02060300",Name:"C相功率因数",Category:"功率因数",DataType:"BCD",DataFormat:"X.XXX",Length:2,Scale:1,Unit:"",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02070100",Name:"A相相角",Category:"电气参数",DataType:"BCD",DataFormat:"XXX.X",Length:2,Scale:1,Unit:"°",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02070200",Name:"B相相角",Category:"电气参数",DataType:"BCD",DataFormat:"XXX.X",Length:2,Scale:1,Unit:"°",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"02070300",Name:"C相相角",Category:"电气参数",DataType:"BCD",DataFormat:"XXX.X",Length:2,Scale:1,Unit:"°",Notes:"常见标准数据项；请核对型号和小数位"},
	{ID:"04000101",Name:"日期及星期",Category:"参变量",DataType:"BCD",DataFormat:"YYMMDDWW",Length:4,Scale:1,Unit:"",Notes:"星期字段0代表星期日"},
	{ID:"04000102",Name:"时间",Category:"参变量",DataType:"BCD",DataFormat:"hhmmss",Length:3,Scale:1,Unit:"",Notes:"返回时分秒字符串"},
	{ID:"04000204",Name:"费率数",Category:"参变量",DataType:"BCD",DataFormat:"NN",Length:1,Scale:1,Unit:"个",Notes:"具体支持范围以电表型号为准"},
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
