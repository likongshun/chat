package config

import (
	"fmt"
	"sync/atomic"
)

// Manager atomically swaps complete validated configuration snapshots.
// Current returns a defensive copy so callers cannot mutate the active snapshot.
type Manager struct {
	current atomic.Pointer[Config]
}

func (m *Manager) Current() *Config {
	current := m.current.Load()
	if current == nil {
		return nil
	}
	copy := cloneConfig(*current)
	return &copy
}

func (m *Manager) ReloadXLSX(path string) error {
	next, err := LoadXLSX(path)
	if err != nil {
		return fmt.Errorf("configuration reload rejected: %w", err)
	}
	m.current.Store(&next)
	return nil
}

func cloneConfig(in Config) Config {
	out := in
	out.Devices = append([]Device(nil), in.Devices...)
	out.Points = append([]Point(nil), in.Points...)
	out.Processing = append([]Processing(nil), in.Processing...)
	out.WriteRules = append([]WriteRule(nil), in.WriteRules...)
	out.Enums = append([]EnumValue(nil), in.Enums...)
	return out
}
