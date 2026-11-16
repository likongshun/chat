//go:build !linux

package wal

import "errors"

func ensureAckReserve(string, int64) error {
	return errors.New("preallocated ACK reserve currently requires Linux fallocate")
}
func releaseAckReserve(string) error {
	return errors.New("preallocated ACK reserve currently requires Linux")
}
