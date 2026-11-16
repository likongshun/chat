package wal

import (
	"path/filepath"
	"testing"
)

func TestSegmentLogRotatesAndReopens(t *testing.T) {
	dir := t.TempDir()
	log, err := openSegmentLog(dir, "segment", 1, 16)
	if err != nil { t.Fatal(err) }
	if err = log.Append([]byte("12345678"), true); err != nil { t.Fatal(err) }
	if err = log.Append([]byte("abcdefgh"), true); err != nil { t.Fatal(err) }
	if log.currentID != 2 { t.Fatalf("current segment=%d, want 2", log.currentID) }
	if err = log.Close(); err != nil { t.Fatal(err) }

	ids, err := listSegmentIDs(filepath.Clean(dir), "segment")
	if err != nil { t.Fatal(err) }
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 { t.Fatalf("segment ids=%v", ids) }

	log, err = openSegmentLog(dir, "segment", 1, 16)
	if err != nil { t.Fatal(err) }
	defer log.Close()
	if log.currentID != 2 { t.Fatalf("reopened segment=%d, want 2", log.currentID) }
}
