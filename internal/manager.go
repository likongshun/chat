package config

import (
    "fmt"
    "sync/atomic"
)

// Manager swaps immutable snapshots atomically. A failed reload leaves the
// previous known-good snapshot active.
type Manager struct { current atomic.Pointer[Snapshot] }
func (m *Manager) Current() *Snapshot { return m.current.Load() }
func (m *Manager) Reload(path string) error {
    next,err:=LoadXLSX(path); if err!=nil{return fmt.Errorf("configuration reload rejected: %w",err)}
    m.current.Store(next); return nil
}
