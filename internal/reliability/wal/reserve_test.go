package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenPreallocatesACKReserve(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	st, err := os.Stat(filepath.Join(dir, "ack.reserve"))
	if err != nil { t.Fatal(err) }
	if st.Size() < 4096 { t.Fatalf("reserve size=%d, want at least 4096", st.Size()) }
}


func TestACKReserveReplacementUsesSyncedTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ack.reserve")
	tmpPath := path + ".tmp"
	if err := os.WriteFile(path, make([]byte, 1024), 0600); err != nil { t.Fatal(err) }
	if err := os.WriteFile(tmpPath, []byte("stale temporary reserve"), 0600); err != nil { t.Fatal(err) }

	if err := ensureAckReserve(dir, 4096); err != nil { t.Fatal(err) }
	st, err := os.Stat(path)
	if err != nil { t.Fatal(err) }
	if st.Size() != 4096 { t.Fatalf("reserve size=%d, want 4096", st.Size()) }
	if _, err = os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("temporary reserve should be renamed away, stat error=%v", err)
	}
}

func TestACKReserveRecreatedAfterEmergencyRelease(t *testing.T) {
	dir := t.TempDir()
	if err := ensureAckReserve(dir, 4096); err != nil { t.Fatal(err) }
	if err := releaseAckReserve(dir); err != nil { t.Fatal(err) }
	if err := ensureAckReserve(dir, 4096); err != nil { t.Fatal(err) }
	st, err := os.Stat(filepath.Join(dir, "ack.reserve"))
	if err != nil { t.Fatal(err) }
	if st.Size() != 4096 { t.Fatalf("reserve size=%d, want 4096", st.Size()) }
}
