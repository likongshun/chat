package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const maxFrameSize = 64 << 20

func writeFrame(f *os.File, payload []byte) error {
	if len(payload) == 0 || len(payload) > maxFrameSize {
		return fmt.Errorf("invalid WAL frame length %d", len(payload))
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if err := writeFull(f, header[:]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if err := writeFull(f, payload); err != nil {
		return fmt.Errorf("write frame payload: %w", err)
	}
	return nil
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// readFrames is strict: incomplete tails and checksum errors are returned.
func readFrames(path string, fn func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		var header [8]byte
		_, err = io.ReadFull(f, header[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: truncated frame header: %w", path, err)
		}
		n := binary.BigEndian.Uint32(header[:4])
		if n == 0 || n > maxFrameSize {
			return fmt.Errorf("%s: invalid frame length %d", path, n)
		}
		payload := make([]byte, int(n))
		if _, err = io.ReadFull(f, payload); err != nil {
			return fmt.Errorf("%s: truncated frame payload: %w", path, err)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
			return fmt.Errorf("%s: frame checksum mismatch", path)
		}
		if err = fn(payload); err != nil {
			return err
		}
	}
}

// repairFrameTail truncates only an incomplete physical tail. A complete frame
// with an invalid length or checksum is treated as corruption, never skipped.
func repairFrameTail(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64
	for {
		var header [8]byte
		n, readErr := io.ReadFull(f, header[:])
		if readErr == io.EOF && n == 0 {
			return nil
		}
		if readErr != nil {
			if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
				if err:=f.Truncate(offset);err!=nil{return err};return syncFile(f)
			}
			return readErr
		}
		length := binary.BigEndian.Uint32(header[:4])
		if length == 0 || length > maxFrameSize {
			return fmt.Errorf("%s: invalid frame length %d at offset %d", path, length, offset)
		}
		payload := make([]byte, int(length))
		if _, readErr = io.ReadFull(f, payload); readErr != nil {
			if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
				if err:=f.Truncate(offset);err!=nil{return err};return syncFile(f)
			}
			return readErr
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
			return fmt.Errorf("%s: frame checksum mismatch at offset %d", path, offset)
		}
		offset += int64(len(header)) + int64(length)
	}
}
