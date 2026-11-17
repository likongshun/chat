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

// ReadOnly returns the immutable active snapshot without copying its slices.
// Callers must never mutate the returned value; use Current when a mutable copy is needed.
func (m *Manager) ReadOnly() *Config {
	return m.current.Load()
}

func (m *Manager) Current() *Config {
	current := m.current.Load()
	if current == nil {
		return nil
	}
	copy := cloneConfig(*current)
	return &copy
}

func NewManager(initial Config) (*Manager, error) {
	manager := &Manager{}
	if err := manager.Reload(initial); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) Reload(next Config) error {
	if err := next.Validate(); err != nil {
		return fmt.Errorf("configuration reload rejected: %w", err)
	}
	copy := cloneConfig(next)
	m.current.Store(&copy)
	return nil
}

func (m *Manager) ReloadXLSX(path string) error {
	next, err := LoadXLSX(path)
	if err != nil {
		return fmt.Errorf("configuration reload rejected: %w", err)
	}
	return m.Reload(next)
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
