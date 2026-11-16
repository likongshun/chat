package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type segmentLog struct {
	dir       string
	prefix    string
	maxBytes  int64
	currentID uint64
	file      *os.File
}

func openSegmentLog(dir, prefix string, firstID uint64, maxBytes int64) (*segmentLog, error) {
	if firstID == 0 { firstID = 1 }
	if maxBytes <= 0 { return nil, fmt.Errorf("segment size must be positive") }
	if err := os.MkdirAll(dir, 0700); err != nil { return nil, err }
	ids, err := listSegmentIDs(dir, prefix)
	if err != nil { return nil, err }
	id := firstID
	if len(ids) > 0 && ids[len(ids)-1] >= id { id = ids[len(ids)-1] }
	log := &segmentLog{dir: dir, prefix: prefix, maxBytes: maxBytes, currentID: id}
	if err = log.openCurrent(); err != nil { return nil, err }
	return log, nil
}

func (s *segmentLog) path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s-%06d.wal", s.prefix, id))
}

func (s *segmentLog) openCurrent() error {
	path := s.path(s.currentID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil { return err }
	if err = repairFrameTail(path); err != nil { f.Close(); return err }
	s.file = f
	return nil
}

func (s *segmentLog) Append(payload []byte, syncWrite bool) error {
	if s.file == nil { return os.ErrClosed }
	st, err := s.file.Stat()
	if err != nil { return err }
	if st.Size() > 0 && st.Size()+int64(8+len(payload)) > s.maxBytes {
		if err = s.Rotate(); err != nil { return err }
	}
	if err = writeFrame(s.file, payload); err != nil { return err }
	if syncWrite { return s.file.Sync() }
	return nil
}

func (s *segmentLog) Rotate() error {
	if s.file == nil { return os.ErrClosed }
	if err := s.file.Sync(); err != nil { return err }
	if err := s.file.Close(); err != nil { return err }
	s.currentID++
	if err := s.openCurrent(); err != nil { return err }
	d, err := os.Open(s.dir)
	if err != nil { return err }
	defer d.Close()
	return d.Sync()
}

func (s *segmentLog) Sync() error {
	if s.file == nil { return os.ErrClosed }
	return s.file.Sync()
}

func (s *segmentLog) Close() error {
	if s.file == nil { return nil }
	err := s.file.Close()
	s.file = nil
	return err
}

func listSegmentIDs(dir, prefix string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil { return nil, err }
	ids := make([]uint64, 0, len(entries))
	start := prefix + "-"
	for _, entry := range entries {
		if entry.IsDir() { continue }
		name := entry.Name()
		if !strings.HasPrefix(name, start) || !strings.HasSuffix(name, ".wal") { continue }
		raw := strings.TrimSuffix(strings.TrimPrefix(name, start), ".wal")
		id, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil || id == 0 { return nil, fmt.Errorf("invalid segment filename %q", name) }
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
