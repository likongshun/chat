package config

import (
	"testing"
	"time"
)

func TestManagerReloadKeepsLastKnownGoodSnapshot(t *testing.T) {
	initial := Config{
		Devices: []Device{{ID: "d1", Name: "device", Host: "127.0.0.1", Port: 502, UnitID: 1, Model: "M", Interval: time.Second, Enabled: true}},
		Points: []Point{{Model: "M", PointCode: "p1", Name: "point", Function: 3, Address: 0, Count: 1, DataType: "UINT16", Scale: 1, Writable: false, Enabled: true}},
	}
	manager, err := NewManager(initial)
	if err != nil {
		t.Fatal(err)
	}
	invalid := initial
	invalid.Devices = []Device{{ID: "d2", Model: "missing", Enabled: true}}
	if err := manager.Reload(invalid); err == nil {
		t.Fatal("expected invalid reload to fail")
	}
	current := manager.Current()
	if current == nil || len(current.Devices) != 1 || current.Devices[0].ID != "d1" {
		t.Fatalf("last known-good snapshot was not retained: %+v", current)
	}
	updated := initial
	updated.Devices = append([]Device(nil), initial.Devices...)
	updated.Devices[0].Name = "renamed"
	if err := manager.Reload(updated); err != nil {
		t.Fatal(err)
	}
	current = manager.Current()
	if current.Devices[0].Name != "renamed" {
		t.Fatalf("updated snapshot not installed: %+v", current.Devices[0])
	}
	current.Devices[0].Name = "mutated by caller"
	if manager.Current().Devices[0].Name != "renamed" {
		t.Fatal("Current returned mutable internal state")
	}
}
