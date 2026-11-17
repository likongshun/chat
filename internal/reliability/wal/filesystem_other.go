//go:build !linux

package wal

func filesystemUsage(string) (usedBytes, totalBytes uint64, err error) {
	return 0, 0, nil
}
