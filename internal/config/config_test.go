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

func TestManagerReloadFailureKeepsLastGoodSnapshot(t *testing.T) {
	var manager Manager
	if err := manager.ReloadXLSX("../../configs/modbus_device_config.xlsx"); err != nil {
		t.Fatal(err)
	}
	before := manager.Current()
	if before == nil || len(before.Points) != 2 {
		t.Fatalf("unexpected initial snapshot: %#v", before)
	}
	if err := manager.ReloadXLSX("does-not-exist.xlsx"); err == nil {
		t.Fatal("expected reload error")
	}
	after := manager.Current()
	if after == nil || len(after.Points) != 2 {
		t.Fatalf("failed reload replaced last-known-good snapshot: %#v", after)
	}
	after.Points[0].PointCode = "mutated"
	if manager.Current().Points[0].PointCode == "mutated" {
		t.Fatal("Current returned mutable internal snapshot")
	}
}


func TestValidateProcessingParameters(t *testing.T) {
	base := Config{
		Points: []Point{{Model: "M", PointCode: "p", Function: 3, Count: 1, DataType: "UINT16", Scale: 1}},
	}
	cases := []struct {
		name string
		rule Processing
	}{
		{name: "bit out of range", rule: Processing{PointCode: "p", Operation: "BIT", Param1: 64}},
		{name: "fractional round precision", rule: Processing{PointCode: "p", Operation: "ROUND", Param1: 1.5}},
		{name: "invalid clamp range", rule: Processing{PointCode: "p", Operation: "CLAMP", Param1: 10, Param2: 1}},
		{name: "zero divide", rule: Processing{PointCode: "p", Operation: "DIVIDE", Param1: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			c.Processing = []Processing{tc.rule}
			if err := c.Validate(); err == nil {
				t.Fatal("expected invalid processing parameters to be rejected")
			}
		})
	}
}

func TestValidateDLT645PointUsesByteLength(t *testing.T) {
	c := Config{
		Devices: []Device{{ID:"m1", Host:"127.0.0.1", Port:8000, Protocol:"dl-t645-2007", MeterAddress:"123456789012", Model:"DLT", Interval:time.Second, Enabled:true}},
		Points: []Point{{ID:"DLT/energy", Model:"DLT", PointCode:"energy", DataID:"00010000", Count:3, DataType:"BCD", Scale:0.01, Enabled:true}},
	}
	if err := c.Validate(); err != nil { t.Fatal(err) }
}

func TestRejectDLT645InvalidDataID(t *testing.T) {
	c := Config{
		Devices: []Device{{ID:"m1", Host:"127.0.0.1", Port:8000, Protocol:"dl-t645-2007", MeterAddress:"123456789012", Model:"DLT", Interval:time.Second, Enabled:true}},
		Points: []Point{{ID:"DLT/energy", Model:"DLT", PointCode:"energy", DataID:"bad", Count:3, DataType:"BCD", Enabled:true}},
	}
	if err := c.Validate(); err == nil { t.Fatal("expected invalid DL/T645 data id") }
}

func TestValidateDLT645DiscoveryOnlyDevice(t *testing.T) {
	c := Config{
		Devices: []Device{{ID:"discover",Host:"127.0.0.1",Port:8000,Protocol:"dl-t645-2007",DiscoveryOnly:true,Model:"DLT",Interval:time.Second,Enabled:true}},
		Points: nil,
	}
	if err := c.Validate(); err != nil { t.Fatal(err) }
	c.Devices[0].DiscoveryOnly = false
	if err := c.Validate(); err == nil { t.Fatal("expected missing meter address rejection") }
}


func TestValidateDLT645DataFormatAndASCII(t *testing.T) {
	base := Config{
		Devices: []Device{{ID:"m1",Host:"127.0.0.1",Port:8000,Protocol:"dl-t645-2007",MeterAddress:"123456789012",Model:"DLT",Interval:time.Second,Enabled:true}},
		Points: []Point{{ID:"DLT/clock",Model:"DLT",PointCode:"clock",DataID:"04000102",Count:3,DataType:"BCD",DataFormat:"hhmmss",Scale:1,Enabled:true}},
	}
	if err := base.Validate(); err != nil { t.Fatalf("valid typed point rejected: %v",err) }
	base.Points[0].DataFormat = "XXXXXX.XX"
	if err := base.Validate(); err == nil { t.Fatal("expected format/length mismatch rejection") }
	base.Points[0] = Point{ID:"DLT/serial",Model:"DLT",PointCode:"serial",DataID:"04000401",Count:4,DataType:"ASCII",DataFormat:"ASCII:4",Scale:1,Enabled:true}
	if err := base.Validate(); err != nil { t.Fatalf("valid ASCII point rejected: %v",err) }
}
