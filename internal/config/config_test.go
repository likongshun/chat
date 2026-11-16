package config

import (
	"testing"
	"time"
)

func TestRejectWritablePoint(t *testing.T) {
	c := Config{Points: []Point{{Model:"M", PointCode:"p", Function:3, Count:1, DataType:"UINT16", Scale:1, Enabled:true, Writable:true}}}
	if err := c.Validate(); err == nil { t.Fatal("expected writable point to be rejected") }
}
func TestRejectRegisterOverflow(t *testing.T) {
	c := Config{Points: []Point{{Model:"M", PointCode:"p", Function:3, Address:65535, Count:2, DataType:"UINT32", Scale:1}}}
	if err := c.Validate(); err == nil { t.Fatal("expected address overflow to be rejected") }
}
func TestValidateWorkbookModelPoint(t *testing.T) {
	c := Config{
		Devices: []Device{{ID:"d1", Host:"127.0.0.1", Port:502, Model:"M", Interval:time.Second, Enabled:true}},
		Points: []Point{{ID:"M/p", Model:"M", PointCode:"p", Function:3, Address:0, Count:2, DataType:"FLOAT32", Scale:1, Enabled:true}},
	}
	if err := c.Validate(); err != nil { t.Fatal(err) }
}

func TestLoadRepositoryWorkbook(t *testing.T) {
	c, err := LoadXLSX("../../configs/modbus_device_config.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Devices) != 1 || len(c.Points) != 2 || len(c.Processing) != 1 || len(c.WriteRules) != 1 || len(c.Enums) != 1 {
		t.Fatalf("unexpected workbook counts: devices=%d points=%d processing=%d write_rules=%d enums=%d",
			len(c.Devices), len(c.Points), len(c.Processing), len(c.WriteRules), len(c.Enums))
	}
}
