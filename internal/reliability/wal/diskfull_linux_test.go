//go:build linux

package wal

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func fillFilesystemUntilENOSPC(t *testing.T, mountDir, name string) string {
	t.Helper()
	path := filepath.Join(mountDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil { t.Fatal(err) }
	block := make([]byte, 64<<10)
	for {
		n, writeErr := f.Write(block)
		if errors.Is(writeErr, syscall.ENOSPC) { break }
		if writeErr != nil { _ = f.Close(); t.Fatalf("fill bounded filesystem: %v", writeErr) }
		if n != len(block) { _ = f.Close(); t.Fatalf("short filler write: %d/%d", n, len(block)) }
	}
	syncErr := f.Sync()
	if syncErr != nil && !errors.Is(syncErr, syscall.ENOSPC) { _ = f.Close(); t.Fatalf("sync filler to force physical allocation: %v", syncErr) }
	if !errors.Is(syncErr, syscall.ENOSPC) && !errors.Is(err, syscall.ENOSPC) {
		_ = f.Close()
		t.Fatal("bounded filesystem did not reach ENOSPC after writing filler")
	}
	if closeErr := f.Close(); closeErr != nil && !errors.Is(closeErr, syscall.ENOSPC) { t.Fatal(closeErr) }
	return path
}

// This test is deliberately opt-in and is enabled by the CI job that mounts
// a small, disposable ext4 loopback filesystem. It exercises real ENOSPC,
// not a mocked syscall error.
func TestWALRealFilesystemFullACKReserve(t *testing.T) {
	mountDir := os.Getenv("WAL_REAL_DISK_FULL_DIR")
	if mountDir == "" { t.Skip("set WAL_REAL_DISK_FULL_DIR to a dedicated bounded filesystem") }
	info, err := os.Stat(mountDir)
	if err != nil || !info.IsDir() { t.Fatalf("invalid disk-full test mount %q: %v", mountDir, err) }

	t.Run("ACK consumes physically preallocated reserve", func(t *testing.T) {
		dir := filepath.Join(mountDir, "wal-reserve-success")
		if err = os.MkdirAll(dir, 0700); err != nil { t.Fatal(err) }
		w, openErr := Open(Options{Dir: dir, SegmentBytes: 8 << 20, MaxBytes: 64 << 20, ReserveBytes: 4 << 20, Sync: true})
		if openErr != nil { t.Fatal(openErr) }
		record, appendErr := w.Append("real-enospc-ack", []byte(`{"v":1}`))
		if appendErr != nil { _ = w.Close(); t.Fatal(appendErr) }
		filler := fillFilesystemUntilENOSPC(t, mountDir, "filler-success.bin")
		defer os.Remove(filler)
		if _, appendErr = w.Append("must-be-rejected", []byte(`{"v":2}`)); !errors.Is(appendErr, ErrCapacity) {
			_ = w.Close()
			t.Fatalf("Data admission at full filesystem must stop, got %v", appendErr)
		}
		if ackErr := w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); ackErr != nil {
			_ = w.Close()
			t.Fatalf("ACK must succeed after releasing preallocated reserve: %v", ackErr)
		}
		state, stateErr := w.ackState()
		if stateErr != nil { _ = w.Close(); t.Fatal(stateErr) }
		if !state["mqtt"][record.Seq] { _ = w.Close(); t.Fatal("ACK was not durable after real ENOSPC recovery") }
		if _, statErr := os.Stat(filepath.Join(dir, "ack.reserve")); !os.IsNotExist(statErr) {
			_ = w.Close()
			t.Fatalf("ACK reserve should have been consumed: %v", statErr)
		}
		if closeErr := w.Close(); closeErr != nil { t.Fatal(closeErr) }
		if removeErr := os.Remove(filler); removeErr != nil { t.Fatal(removeErr) }
	})

	t.Run("ACK fails closed after reserve exhaustion", func(t *testing.T) {
		dir := filepath.Join(mountDir, "wal-reserve-exhausted")
		if err = os.MkdirAll(dir, 0700); err != nil { t.Fatal(err) }
		w, openErr := Open(Options{Dir: dir, SegmentBytes: 8 << 20, MaxBytes: 64 << 20, ReserveBytes: 4 << 20, Sync: true})
		if openErr != nil { t.Fatal(openErr) }
		record, appendErr := w.Append("real-enospc-exhausted", []byte(`{"v":1}`))
		if appendErr != nil { _ = w.Close(); t.Fatal(appendErr) }
		if err = releaseAckReserve(dir); err != nil { _ = w.Close(); t.Fatal(err) }
		filler := fillFilesystemUntilENOSPC(t, mountDir, "filler-exhausted.bin")
		defer os.Remove(filler)
		ackErr := w.Ack(Ack{Seq: record.Seq, Sink: "influx", EventID: record.EventID})
		if !errors.Is(ackErr, syscall.ENOSPC) {
			_ = w.Close()
			t.Fatalf("ACK must report real ENOSPC after reserve exhaustion, got %v", ackErr)
		}
		state, stateErr := w.ackState()
		if stateErr != nil { _ = w.Close(); t.Fatal(stateErr) }
		if state["influx"][record.Seq] { _ = w.Close(); t.Fatal("ACK was falsely marked durable after reserve exhaustion") }
		if closeErr := w.Close(); closeErr != nil { t.Fatal(closeErr) }
		if removeErr := os.Remove(filler); removeErr != nil { t.Fatal(removeErr) }
	})
}

