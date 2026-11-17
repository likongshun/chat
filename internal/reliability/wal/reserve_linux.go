//go:build linux

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func ensureAckReserve(dir string, size int64) error {
	if size <= 0 {
		return nil
	}
	path := filepath.Join(dir, "ack.reserve")
	if st, err := os.Stat(path); err == nil && st.Size() >= size {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Build a fully allocated replacement beside the current reserve. The old
	// reserve remains intact until the new file is allocated and synced.
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	cleanup := func(cause error) error {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return cause
	}
	if err = syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return cleanup(fmt.Errorf("preallocate ACK reserve (%d bytes): %w", size, err))
	}
	if err = syncFile(f); err != nil {
		return cleanup(fmt.Errorf("sync ACK reserve: %w", err))
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err = os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publish ACK reserve: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err = syncFile(d); err != nil {
		return fmt.Errorf("sync ACK reserve directory: %w", err)
	}
	return nil
}

func releaseAckReserve(dir string) error {
	path := filepath.Join(dir, "ack.reserve")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return syncFile(d)
}
