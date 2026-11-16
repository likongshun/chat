package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRepairFrameTailTruncatesPartialHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil { t.Fatal(err) }
	if err = writeFrame(f, []byte(`{"seq":1}`)); err != nil { t.Fatal(err) }
	if _, err = f.Write([]byte{0, 0, 0}); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }

	if err = repairFrameTail(path); err != nil { t.Fatal(err) }
	var got int
	if err = readFrames(path, func([]byte) error { got++; return nil }); err != nil { t.Fatal(err) }
	if got != 1 { t.Fatalf("frames=%d, want 1", got) }
}

func TestRepairFrameTailRejectsChecksumCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil { t.Fatal(err) }
	if err = writeFrame(f, []byte(`{"seq":1}`)); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }

	f, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil { t.Fatal(err) }
	if _, err = f.WriteAt([]byte{0xff}, 8); err != nil { t.Fatal(err) }
	_ = f.Close()
	if err = repairFrameTail(path); err == nil { t.Fatal("expected checksum corruption error") }
}
