//go:build linux

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func ensureAckReserve(dir string, size int64) error {
	if size <= 0 { return nil }
	path := filepath.Join(dir, "ack.reserve")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil { return err }
	defer f.Close()
	st, err := f.Stat()
	if err != nil { return err }
	if st.Size() >= size { return nil }
	if err = syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("preallocate ACK reserve (%d bytes): %w", size, err)
	}
	return f.Sync()
}

func releaseAckReserve(dir string) error {
	path := filepath.Join(dir, "ack.reserve")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) { return err }
	d, err := os.Open(dir)
	if err != nil { return err }
	defer d.Close()
	return d.Sync()
}
