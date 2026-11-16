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
