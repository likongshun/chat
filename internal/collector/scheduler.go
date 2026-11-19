package collector

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strings"
	"sort"
	"sync"
	"time"

	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/telemetry"
)

type PollFunc func(context.Context, config.Device) ([]Sample, error)

type Result struct {
	DeviceID  string
	At        time.Time
	StartedAt time.Time
	Duration  time.Duration
	Samples   []Sample
	Err       error
}

type Scheduler struct {
	// Workers is the maximum number of devices polled concurrently.
	Workers       int
	Timeout       time.Duration
	OutputBuffer  int
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	InitialStagger bool
	DisableInitialStagger bool
	Metrics       *Metrics
	DeviceStats   *telemetry.CommunicationStats
}

func (s Scheduler) defaults() Scheduler {
	if s.Workers <= 0 { s.Workers = 500 }
	if s.Timeout <= 0 { s.Timeout = 800 * time.Millisecond }
	if s.OutputBuffer <= 0 { s.OutputBuffer = 10000 }
	if s.MinBackoff <= 0 { s.MinBackoff = time.Second }
	if s.MaxBackoff <= 0 { s.MaxBackoff = 30 * time.Second }
	if s.MaxBackoff < s.MinBackoff { s.MaxBackoff = s.MinBackoff }
	s.InitialStagger = !s.DisableInitialStagger
	if s.Metrics == nil { s.Metrics = &Metrics{} }
	return s
}

// Run starts one serialized polling loop per enabled device. The global
// semaphore bounds simultaneous network work. Results use a bounded channel;
// when consumers fall behind, workers backpressure instead of silently
// dropping acquired samples.
func (s Scheduler) Run(ctx context.Context, devices []config.Device, poll PollFunc) <-chan Result {
	s = s.defaults()
	out := make(chan Result, s.OutputBuffer)
	if ctx == nil { ctx = context.Background() }
	var wg sync.WaitGroup
	limit := make(chan struct{}, s.Workers)
	enabledIndex := 0
	for _, device := range devices {
		if !device.Enabled { continue }
		index := enabledIndex
		enabledIndex++
		wg.Add(1)
		go func(d config.Device, i int) {
			defer wg.Done()
			s.runDevice(ctx, limit, out, d, i, poll)
		}(device, index)
	}
	go func() { wg.Wait(); close(out) }()
	return out
}

func (s Scheduler) runDevice(ctx context.Context, limit chan struct{}, out chan<- Result, d config.Device, index int, poll PollFunc) {
	interval := d.Interval
	if interval <= 0 { interval = time.Second }
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(index)*7919))
	next := time.Now()
	if s.InitialStagger {
		staggerSlots := s.Workers
		if staggerSlots < 1 { staggerSlots = 1 }
		next = next.Add(time.Duration(index%staggerSlots) * interval / time.Duration(staggerSlots))
	}
	failures := 0
	for {
		if wait := time.Until(next); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !timer.Stop() { select { case <-timer.C: default: } }
				return
			case <-timer.C:
			}
		}
		if ctx.Err() != nil { return }
		select {
		case limit <- struct{}{}:
		case <-ctx.Done():
			return
		}
		started := time.Now()
		callCtx, cancel := context.WithTimeout(ctx, s.Timeout)
		s.Metrics.inflight.Add(1)
		var samples []Sample
		var err error
		if poll == nil {
			err = errors.New("nil poll function")
		} else {
			samples, err = poll(callCtx, d)
		}
		cancel()
		s.Metrics.inflight.Add(-1)
		<-limit
		finished := time.Now()
		result := Result{DeviceID:d.ID, At:finished, StartedAt:started, Duration:finished.Sub(started), Samples:samples, Err:err}
		s.Metrics.record(result)
		if s.DeviceStats != nil {
			code, timeout := communicationErrorCode(err)
			s.DeviceStats.Record(d.ID, finished, result.Duration, code, timeout, false)
		}
		select {
		case out <- result:
		case <-ctx.Done():
			return
		}
		if err != nil {
			failures++
			next = time.Now().Add(backoff(s.MinBackoff, s.MaxBackoff, failures, rng))
		} else {
			failures = 0
			next = started.Add(interval)
			if !next.After(time.Now()) { next = time.Now().Add(interval) }
		}
	}
}

func backoff(minimum, maximum time.Duration, failures int, rng *rand.Rand) time.Duration {
	if failures < 1 { failures = 1 }
	d := minimum
	for i:=1; i<failures && d<maximum; i++ {
		if d > maximum/2 { d = maximum; break }
		d *= 2
	}
	if d > maximum { d = maximum }
	// 0.8..1.2 jitter prevents a fleet-wide reconnect herd.
	factor := 0.8 + rng.Float64()*0.4
	return time.Duration(float64(d)*factor)
}

// Poll executes one bounded polling pass. It is retained for callers that
// need a one-shot sweep; long-running collection should use Run.
func (s Scheduler) Poll(ctx context.Context, devices []config.Device, fn func(context.Context, config.Device) error) []Result {
	s = s.defaults()
	if ctx == nil { ctx = context.Background() }
	jobs := make(chan config.Device)
	out := make(chan Result, len(devices))
	var wg sync.WaitGroup
	for i:=0; i<s.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs {
				started := time.Now()
				callCtx, cancel := context.WithTimeout(ctx, s.Timeout)
				var err error
				if fn == nil { err = errors.New("nil poll function") } else { err = fn(callCtx, d) }
				cancel()
				out <- Result{DeviceID:d.ID, At:time.Now(), StartedAt:started, Duration:time.Since(started), Err:err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, d := range devices {
			if !d.Enabled { continue }
			select { case jobs <- d: case <-ctx.Done(): return }
		}
	}()
	go func() { wg.Wait(); close(out) }()
	var results []Result
	for r := range out { results = append(results, r) }
	sort.Slice(results, func(i,j int) bool { return results[i].StartedAt.Before(results[j].StartedAt) })
	return results
}

func communicationErrorCode(err error) (string, bool) {
 if err==nil { return "",false }
 if errors.Is(err,context.DeadlineExceeded) { return "TIMEOUT",true }
 if errors.Is(err,context.Canceled) { return "CANCELED",false }
 var netErr net.Error
 if errors.As(err,&netErr) && netErr.Timeout() { return "TIMEOUT",true }
 message:=strings.ToLower(err.Error())
 switch {
 case strings.Contains(message,"crc"),strings.Contains(message,"checksum"):
  return "CRC_ERROR",false
 case strings.Contains(message,"decode"),strings.Contains(message,"bcd"):
  return "DECODE_ERROR",false
 case strings.Contains(message,"protocol"):
  return "PROTOCOL_ERROR",false
 default:
  return "COMMUNICATION_ERROR",false
 }
}
