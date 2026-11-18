package dlt645

import (
	"testing"

	"go.bug.st/serial"
)

func TestParseSerialEndpoint(t *testing.T) {
	device, mode, err := parseSerialEndpoint("serial:///dev/ttyUSB0?baud=2400&data_bits=8&parity=even&stop_bits=1")
	if err != nil {
		t.Fatal(err)
	}
	if device != "/dev/ttyUSB0" {
		t.Fatalf("device=%q", device)
	}
	if mode.BaudRate != 2400 || mode.DataBits != 8 || mode.Parity != serial.EvenParity || mode.StopBits != serial.OneStopBit {
		t.Fatalf("unexpected serial mode: %#v", mode)
	}
}

func TestParseSerialEndpointDefaults(t *testing.T) {
	_, mode, err := parseSerialEndpoint("serial:///dev/serial/by-id/usb-rs485")
	if err != nil {
		t.Fatal(err)
	}
	if mode.BaudRate != 9600 || mode.DataBits != 8 || mode.Parity != serial.NoParity || mode.StopBits != serial.OneStopBit {
		t.Fatalf("unexpected defaults: %#v", mode)
	}
}

func TestParseSerialEndpointRejectsUnsafeOrInvalidValues(t *testing.T) {
	cases := []string{
		"tcp://127.0.0.1:502",
		"serial:///etc/passwd",
		"serial:///dev/../etc/passwd",
		"serial:///dev/ttyUSB0?baud=bad",
		"serial:///dev/ttyUSB0?parity=mark",
		"serial:///dev/ttyUSB0?data_bits=9",
		"serial:///dev/ttyUSB0?stop_bits=3",
		"serial:///dev/ttyUSB0?unknown=1",
	}
	for _, endpoint := range cases {
		t.Run(endpoint, func(t *testing.T) {
			if _, _, err := parseSerialEndpoint(endpoint); err == nil {
				t.Fatalf("expected endpoint rejection: %s", endpoint)
			}
		})
	}
}
