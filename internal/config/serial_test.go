package config

import (
	"testing"
	"time"
)

func TestValidateDLT645SerialDiscoveryDevice(t *testing.T) {
	c := Config{Devices: []Device{{
		ID: "serial-meter", Protocol: "dl-t645-2007", Transport: "serial",
		SerialPort: "/dev/ttyUSB0", BaudRate: 2400, DataBits: 8,
		Parity: "even", StopBits: 1, DiscoveryOnly: true,
		Model: "DLT", Interval: time.Second, Enabled: true,
	}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectInvalidDLT645SerialConfiguration(t *testing.T) {
	cases := []Device{
		{ID: "bad-path", Protocol: "dl-t645-2007", Transport: "serial", SerialPort: "/etc/passwd", Model: "DLT", Interval: time.Second},
		{ID: "bad-baud", Protocol: "dl-t645-2007", Transport: "serial", SerialPort: "/dev/ttyUSB0", BaudRate: 0, DataBits: 9, Model: "DLT", Interval: time.Second},
		{ID: "wrong-protocol", Protocol: "modbus-tcp", Transport: "serial", SerialPort: "/dev/ttyUSB0", Model: "M", Interval: time.Second},
	}
	for _, device := range cases {
		t.Run(device.ID, func(t *testing.T) {
			if err := (Config{Devices: []Device{device}}).Validate(); err == nil {
				t.Fatal("expected invalid serial configuration to be rejected")
			}
		})
	}
}
