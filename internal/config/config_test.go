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
