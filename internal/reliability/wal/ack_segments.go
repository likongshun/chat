package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func ackSegmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("ack-%020d.wal", id))
}

func readAckFrames(dir string, manifest Manifest, visit func([]byte) error) error {
	if manifest.AckFirstSegment == 0 || manifest.AckActiveSegment == 0 {
		return readFrames(filepath.Join(dir, "ack.wal"), visit)
	}
	if manifest.AckFirstSegment > manifest.AckActiveSegment {
		return fmt.Errorf("invalid ACK segment range %d..%d", manifest.AckFirstSegment, manifest.AckActiveSegment)
	}
	for id := manifest.AckFirstSegment; id <= manifest.AckActiveSegment; id++ {
		path := ackSegmentPath(dir, id)
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("required ACK segment %d is unavailable: %w", id, err)
		}
		if err := readFrames(path, visit); err != nil {
			return fmt.Errorf("read ACK segment %d: %w", id, err)
		}
		if id == ^uint64(0) {
			break
		}
	}
	return nil
}

// prepareAckSegments upgrades the legacy ack.wal file without discarding its
// source. The new segment and directory entry are durable before MANIFEST
// starts referencing them; the legacy file is removed only after that commit.
func (w *WAL) prepareAckSegments() error {
	if w.manifest.AckFirstSegment != 0 || w.manifest.AckActiveSegment != 0 {
		if w.manifest.AckFirstSegment == 0 || w.manifest.AckActiveSegment < w.manifest.AckFirstSegment {
			return fmt.Errorf("invalid ACK segment boundaries")
		}
		return nil
	}

	legacy := filepath.Join(w.opts.Dir, "ack.wal")
	segment := ackSegmentPath(w.opts.Dir, 1)
	tmp := segment + ".tmp"
	source, err := os.Open(legacy)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if source != nil {
		defer source.Close()
	}

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if source != nil {
		if _, err = io.Copy(f, source); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err = syncFile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, segment); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	d, err := os.Open(w.opts.Dir)
	if err != nil {
		return err
	}
	err = syncFile(d)
	_ = d.Close()
	if err != nil {
		return err
	}

	w.manifest.AckFirstSegment = 1
	w.manifest.AckActiveSegment = 1
	if err = w.saveManifest(); err != nil {
		return err
	}
	if err = os.Remove(legacy); err != nil && !os.IsNotExist(err) {
		return err
	}
	d, err = os.Open(w.opts.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return syncFile(d)
}

func (w *WAL) cleanupObsoleteAckSegments() error {
	if w.manifest.AckFirstSegment == 0 {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(w.opts.Dir, "ack-*.wal"))
	if err != nil {
		return err
	}
	changed := false
	for _, path := range files {
		var id uint64
		if _, err = fmt.Sscanf(filepath.Base(path), "ack-%d.wal", &id); err != nil {
			return err
		}
		if id < w.manifest.AckFirstSegment || id > w.manifest.AckActiveSegment {
			if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			changed = true
		}
	}
	if changed {
		d, err := os.Open(w.opts.Dir)
		if err != nil {
			return err
		}
		defer d.Close()
		return syncFile(d)
	}
	return nil
}
