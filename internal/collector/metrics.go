package collector

import (
	"context"
	"sync/atomic"
	"time"
)

type Metrics struct {
	pollsTotal       atomic.Uint64
	pollErrorsTotal  atomic.Uint64
	pollTimeoutsTotal atomic.Uint64
	samplesTotal     atomic.Uint64
	inflight         atomic.Int64
	totalDurationNS  atomic.Uint64
	maxDurationNS    atomic.Uint64
	lastPollUnixNano atomic.Int64
}

type MetricsSnapshot struct {
	PollsTotal        uint64
	PollErrorsTotal   uint64
	PollTimeoutsTotal uint64
	SamplesTotal      uint64
	Inflight          int64
	TotalDuration     time.Duration
	MaxDuration       time.Duration
	LastPoll          time.Time
}

func (m *Metrics) record(r Result) {
	m.pollsTotal.Add(1)
	m.samplesTotal.Add(uint64(len(r.Samples)))
	m.totalDurationNS.Add(uint64(r.Duration))
	for {
		old := m.maxDurationNS.Load()
		if uint64(r.Duration) <= old || m.maxDurationNS.CompareAndSwap(old, uint64(r.Duration)) { break }
	}
	m.lastPollUnixNano.Store(r.At.UnixNano())
	if r.Err != nil {
		m.pollErrorsTotal.Add(1)
		if r.Err == context.DeadlineExceeded || r.Err == context.Canceled {
			m.pollTimeoutsTotal.Add(1)
		}
	}
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	last := m.lastPollUnixNano.Load()
	var lastTime time.Time
	if last != 0 { lastTime = time.Unix(0,last) }
	return MetricsSnapshot{
		PollsTotal:m.pollsTotal.Load(),
		PollErrorsTotal:m.pollErrorsTotal.Load(),
		PollTimeoutsTotal:m.pollTimeoutsTotal.Load(),
		SamplesTotal:m.samplesTotal.Load(),
		Inflight:m.inflight.Load(),
		TotalDuration:time.Duration(m.totalDurationNS.Load()),
		MaxDuration:time.Duration(m.maxDurationNS.Load()),
		LastPoll:lastTime,
	}
}
