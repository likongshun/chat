package config

import (
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestReadDevicesParsesNativeSerialSettings(t *testing.T) {
	f := excelize.NewFile()
	defer f.Close()
	if err := f.SetSheetName("Sheet1", "Devices"); err != nil {
		t.Fatal(err)
	}
	headers := []interface{}{
		"device_id", "host", "port", "unit_id", "model", "poll_ms", "enabled",
		"protocol", "transport", "serial_port", "baud_rate", "data_bits", "parity", "stop_bits", "discovery_only",
	}
	values := []interface{}{
		"meter-serial", "", "0", "1", "DLT", "1000", "TRUE",
		"dl-t645-2007", "serial", "/dev/ttyUSB0", "2400", "8", "even", "1", "TRUE",
	}
	if err := f.SetSheetRow("Devices", "A1", &headers); err != nil {
		t.Fatal(err)
	}
	if err := f.SetSheetRow("Devices", "A2", &values); err != nil {
		t.Fatal(err)
	}
	devices, err := readDevices(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 1 {
		t.Fatalf("got %d devices, want 1", len(devices))
	}
	d := devices[0]
	if d.Transport != "serial" || d.SerialPort != "/dev/ttyUSB0" || d.BaudRate != 2400 ||
		d.DataBits != 8 || d.Parity != "even" || d.StopBits != 1 || !d.DiscoveryOnly {
		t.Fatalf("serial settings not parsed: %#v", d)
	}
}
