//go:build linux

package wal

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestACKReserveIsPhysicallyPreallocatedAndReleasable(t *testing.T) {
	dir := t.TempDir()
	const reserveBytes = int64(2 << 20)
	if err := ensureAckReserve(dir, reserveBytes); err != nil {
		t.Fatalf("preallocate ACK reserve: %v", err)
	}
	path := filepath.Join(dir, "ack.reserve")
	info, err := os.Stat(path)
	if err != nil { t.Fatal(err) }
	if info.Size() != reserveBytes { t.Fatalf("reserve size=%d, want %d", info.Size(), reserveBytes) }
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok { t.Fatal("expected Linux stat metadata") }
	if allocated := stat.Blocks * 512; allocated < reserveBytes {
		t.Fatalf("ACK reserve is sparse: allocated=%d bytes, requested=%d", allocated, reserveBytes)
	}
	if err = releaseAckReserve(dir); err != nil { t.Fatal(err) }
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("ACK reserve was not released: %v", err)
	}
}
