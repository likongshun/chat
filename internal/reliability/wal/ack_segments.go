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

func (w *WAL) rotateAckSegment() error {
	if w.manifest.AckActiveSegment == ^uint64(0) {
		return fmt.Errorf("ACK segment sequence exhausted")
	}
	oldID := w.manifest.AckActiveSegment
	newID := oldID + 1
	path := ackSegmentPath(w.opts.Dir, newID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil && !os.IsExist(err) {
		return err
	}
	if os.IsExist(err) {
		f, err = os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil { return err }
		st, statErr := f.Stat()
		if statErr != nil { _ = f.Close(); return statErr }
		if st.Size() != 0 { _ = f.Close(); return fmt.Errorf("orphan ACK segment %d is not empty", newID) }
	}
	if err = syncFile(f); err != nil { _ = f.Close(); return err }
	if err = f.Close(); err != nil { return err }
	d, err := os.Open(w.opts.Dir)
	if err != nil { return err }
	err = syncFile(d)
	_ = d.Close()
	if err != nil { return fmt.Errorf("sync directory after ACK segment creation: %w", err) }

	w.manifest.AckActiveSegment = newID
	if err = w.saveManifest(); err != nil {
		w.manifest.AckActiveSegment = oldID
		return fmt.Errorf("commit ACK segment %d: %w", newID, err)
	}
	if w.ack != nil {
		if err = w.ack.Close(); err != nil { w.ack = nil; return err }
	}
	w.ack, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil { return err }
	return nil
}

func (w *WAL) compactAckSegments(boundary uint64) error {
	if w.manifest.AckActiveSegment == ^uint64(0) {
		return fmt.Errorf("ACK segment sequence exhausted")
	}
	newID := w.manifest.AckActiveSegment + 1
	path := ackSegmentPath(w.opts.Dir, newID)
	tmpPath := path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil { return err }
	keepErr := readAckFrames(w.opts.Dir, w.manifest, func(b []byte) error {
		var ack Ack
		if err := json.Unmarshal(b, &ack); err != nil { return err }
		if ack.Seq > boundary { return writeFrame(tmp, b) }
		return nil
	})
	if keepErr != nil { _ = tmp.Close(); _ = os.Remove(tmpPath); return keepErr }
	if err = syncFile(tmp); err != nil { _ = tmp.Close(); _ = os.Remove(tmpPath); return err }
	if err = tmp.Close(); err != nil { _ = os.Remove(tmpPath); return err }
	if err = os.Rename(tmpPath, path); err != nil { _ = os.Remove(tmpPath); return err }
	d, err := os.Open(w.opts.Dir)
	if err != nil { return err }
	err = syncFile(d)
	_ = d.Close()
	if err != nil { return err }

	oldFirst, oldActive := w.manifest.AckFirstSegment, w.manifest.AckActiveSegment
	w.manifest.AckFirstSegment, w.manifest.AckActiveSegment = newID, newID
	if err = w.saveManifest(); err != nil {
		w.manifest.AckFirstSegment, w.manifest.AckActiveSegment = oldFirst, oldActive
		return fmt.Errorf("commit compacted ACK segment: %w", err)
	}

	if w.ack != nil {
		closeErr := w.ack.Close()
		w.ack = nil
		if closeErr != nil { return closeErr }
	}
	w.ack, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil { return err }
	return w.cleanupObsoleteAckSegments()
}
