//go:build linux

package wal

import "syscall"

func filesystemUsage(path string) (usedBytes, totalBytes uint64, err error) {
	var stat syscall.Statfs_t
	if err = syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	blockSize := uint64(stat.Bsize)
	totalBytes = uint64(stat.Blocks) * blockSize
	availableBytes := uint64(stat.Bavail) * blockSize
	if availableBytes > totalBytes {
		availableBytes = totalBytes
	}
	usedBytes = totalBytes - availableBytes
	return usedBytes, totalBytes, nil
}
