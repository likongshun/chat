package wal

import (
	"os"
	"sync/atomic"
	"time"
)

var fsyncBucketBounds = [...]time.Duration{
	time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
	250 * time.Millisecond, time.Second,
}

type fsyncMetrics struct {
	count atomic.Uint64
	errors atomic.Uint64
	totalNanos atomic.Uint64
	maxNanos atomic.Uint64
	buckets [9]atomic.Uint64
}

var processFsyncMetrics fsyncMetrics

// syncFile is the single fsync instrumentation point for WAL files and directories.
func syncFile(f *os.File) error {
	start := time.Now()
	err := f.Sync()
	elapsed := time.Since(start)
	nanos := uint64(elapsed)
	processFsyncMetrics.count.Add(1)
	processFsyncMetrics.totalNanos.Add(nanos)
	if err != nil {
		processFsyncMetrics.errors.Add(1)
	}
	for i, bound := range fsyncBucketBounds {
		if elapsed <= bound {
			processFsyncMetrics.buckets[i].Add(1)
			return err
		}
	}
	processFsyncMetrics.buckets[len(fsyncBucketBounds)].Add(1)
	for {
		old := processFsyncMetrics.maxNanos.Load()
		if nanos <= old || processFsyncMetrics.maxNanos.CompareAndSwap(old, nanos) {
			break
		}
	}
	return err
}

type FsyncStats struct {
	Count uint64
	Errors uint64
	Total time.Duration
	Max time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Buckets [9]uint64
}

func snapshotFsyncMetrics() FsyncStats {
	s := FsyncStats{
		Count: processFsyncMetrics.count.Load(),
		Errors: processFsyncMetrics.errors.Load(),
		Total: time.Duration(processFsyncMetrics.totalNanos.Load()),
		Max: time.Duration(processFsyncMetrics.maxNanos.Load()),
	}
	for i := range s.Buckets {
		s.Buckets[i] = processFsyncMetrics.buckets[i].Load()
	}
	s.P50 = fsyncQuantile(s, 0.50)
	s.P95 = fsyncQuantile(s, 0.95)
	s.P99 = fsyncQuantile(s, 0.99)
	return s
}

func fsyncQuantile(s FsyncStats, q float64) time.Duration {
	if s.Count == 0 { return 0 }
	target := uint64(float64(s.Count)*q)
	if target == 0 { target = 1 }
	var cumulative uint64
	for i, count := range s.Buckets {
		cumulative += count
		if cumulative >= target {
			if i < len(fsyncBucketBounds) { return fsyncBucketBounds[i] }
			return s.Max
		}
	}
	return s.Max
}
