package telemetry

import (
 "sync"
 "sync/atomic"
 "time"
)

var latencyBounds = [...]time.Duration{
 10*time.Millisecond,25*time.Millisecond,50*time.Millisecond,
 100*time.Millisecond,250*time.Millisecond,500*time.Millisecond,
 time.Second,2*time.Second,5*time.Second,10*time.Second,
}

type CommunicationSnapshot struct {
 DeviceID string `json:"device_id"`
 Attempts uint64 `json:"attempts"`
 Successes uint64 `json:"successes"`
 Failures uint64 `json:"failures"`
 Timeouts uint64 `json:"timeouts"`
 ConsecutiveFailures uint64 `json:"consecutive_failures"`
 Reconnects uint64 `json:"reconnects"`
 LatencyCount uint64 `json:"latency_count"`
 LatencyTotal time.Duration `json:"latency_total"`
 LatencyMax time.Duration `json:"latency_max"`
 LastAttempt time.Time `json:"last_attempt,omitempty"`
 LastSuccess time.Time `json:"last_success,omitempty"`
 LastError time.Time `json:"last_error,omitempty"`
 LastErrorCode string `json:"last_error_code,omitempty"`
 LatencyBuckets []uint64 `json:"latency_buckets"`
}

type deviceCounters struct {
 attempts atomic.Uint64
 successes atomic.Uint64
 failures atomic.Uint64
 timeouts atomic.Uint64
 consecutive atomic.Uint64
 reconnects atomic.Uint64
 latencyCount atomic.Uint64
 latencyTotal atomic.Uint64
 latencyMax atomic.Uint64
 buckets [len(latencyBounds)+1]atomic.Uint64
 mu sync.RWMutex
 lastAttempt time.Time
 lastSuccess time.Time
 lastError time.Time
 lastErrorCode string
}

type protocolCounters struct {
 attempts atomic.Uint64
 successes atomic.Uint64
 failures atomic.Uint64
 timeouts atomic.Uint64
 latencyTotal atomic.Uint64
}

type ProtocolCommunicationSnapshot struct {
 Protocol string
 Attempts uint64
 Successes uint64
 Failures uint64
 Timeouts uint64
 LatencyTotal time.Duration
}

type CommunicationStats struct {
 mu sync.RWMutex
 devices map[string]*deviceCounters
 protocols map[string]*protocolCounters
}

func NewCommunicationStats() *CommunicationStats {
 return &CommunicationStats{devices:make(map[string]*deviceCounters),protocols:make(map[string]*protocolCounters)}
}

func (s *CommunicationStats) counters(deviceID string) *deviceCounters {
 s.mu.RLock()
 c:=s.devices[deviceID]
 s.mu.RUnlock()
 if c!=nil { return c }
 s.mu.Lock()
 defer s.mu.Unlock()
 if c=s.devices[deviceID]; c==nil {
  c=&deviceCounters{}
  s.devices[deviceID]=c
 }
 return c
}

// Record stores per-device diagnostics. Device IDs must not be Prometheus labels.
func (s *CommunicationStats) Record(deviceID string, at time.Time, latency time.Duration, errCode string, timeout,reconnect bool) {
 if s==nil || deviceID=="" { return }
 c:=s.counters(deviceID)
 if at.IsZero() { at=time.Now().UTC() }
 if latency<0 { latency=0 }
 c.attempts.Add(1)
 c.latencyCount.Add(1)
 c.latencyTotal.Add(uint64(latency))
 for {
  old:=c.latencyMax.Load()
  if uint64(latency)<=old || c.latencyMax.CompareAndSwap(old,uint64(latency)) { break }
 }
 bucket:=len(latencyBounds)
 for i,bound:=range latencyBounds { if latency<=bound { bucket=i; break } }
 c.buckets[bucket].Add(1)
 c.mu.Lock()
 c.lastAttempt=at.UTC()
 if errCode=="" {
  c.successes.Add(1)
  c.consecutive.Store(0)
  c.lastSuccess=at.UTC()
 } else {
  c.failures.Add(1)
  c.consecutive.Add(1)
  c.lastError=at.UTC()
  c.lastErrorCode=errCode
  if timeout { c.timeouts.Add(1) }
 }
 if reconnect { c.reconnects.Add(1) }
 c.mu.Unlock()
}

func (s *CommunicationStats) Snapshot(deviceID string) (CommunicationSnapshot,bool) {
 if s==nil { return CommunicationSnapshot{},false }
 s.mu.RLock()
 c:=s.devices[deviceID]
 s.mu.RUnlock()
 if c==nil { return CommunicationSnapshot{},false }
 c.mu.RLock()
 out:=CommunicationSnapshot{
  DeviceID:deviceID,Attempts:c.attempts.Load(),Successes:c.successes.Load(),
  Failures:c.failures.Load(),Timeouts:c.timeouts.Load(),
  ConsecutiveFailures:c.consecutive.Load(),Reconnects:c.reconnects.Load(),
  LatencyCount:c.latencyCount.Load(),LatencyTotal:time.Duration(c.latencyTotal.Load()),
  LatencyMax:time.Duration(c.latencyMax.Load()),LastAttempt:c.lastAttempt,
  LastSuccess:c.lastSuccess,LastError:c.lastError,LastErrorCode:c.lastErrorCode,
  LatencyBuckets:make([]uint64,len(c.buckets)),
 }
 for i:=range c.buckets { out.LatencyBuckets[i]=c.buckets[i].Load() }
 c.mu.RUnlock()
 return out,true
}

func (s *CommunicationStats) DeviceCount() int {
 if s==nil { return 0 }
 s.mu.RLock()
 defer s.mu.RUnlock()
 return len(s.devices)
}


type FleetCommunicationSnapshot struct {
 Devices int
 Attempts uint64
 Successes uint64
 Failures uint64
 Timeouts uint64
 Reconnects uint64
 LatencyCount uint64
 LatencyTotal time.Duration
}

func (s *CommunicationStats) FleetSnapshot() FleetCommunicationSnapshot {
 out:=FleetCommunicationSnapshot{}
 if s==nil { return out }
 s.mu.RLock()
 devices:=make([]*deviceCounters,0,len(s.devices))
 for _,c:=range s.devices { devices=append(devices,c) }
 out.Devices=len(devices)
 s.mu.RUnlock()
 for _,c:=range devices {
  out.Attempts+=c.attempts.Load()
  out.Successes+=c.successes.Load()
  out.Failures+=c.failures.Load()
  out.Timeouts+=c.timeouts.Load()
  out.Reconnects+=c.reconnects.Load()
  out.LatencyCount+=c.latencyCount.Load()
  out.LatencyTotal+=time.Duration(c.latencyTotal.Load())
 }
 return out
}

func (s *CommunicationStats) RecordProtocol(protocol,deviceID string,at time.Time,latency time.Duration,errCode string,timeout,reconnect bool) {
 if s==nil { return }
 if protocol=="" { protocol=string(ProtocolModbusTCP) }
 s.Record(deviceID,at,latency,errCode,timeout,reconnect)
 s.mu.Lock()
 p:=s.protocols[protocol]
 if p==nil { p=&protocolCounters{};s.protocols[protocol]=p }
 s.mu.Unlock()
 p.attempts.Add(1)
 p.latencyTotal.Add(uint64(maxDuration(latency,0)))
 if errCode=="" { p.successes.Add(1) } else {
  p.failures.Add(1)
  if timeout { p.timeouts.Add(1) }
 }
}

func (s *CommunicationStats) ProtocolSnapshots() []ProtocolCommunicationSnapshot {
 if s==nil { return nil }
 s.mu.RLock()
 out:=make([]ProtocolCommunicationSnapshot,0,len(s.protocols))
 for name,p:=range s.protocols {
  out=append(out,ProtocolCommunicationSnapshot{Protocol:name,Attempts:p.attempts.Load(),Successes:p.successes.Load(),Failures:p.failures.Load(),Timeouts:p.timeouts.Load(),LatencyTotal:time.Duration(p.latencyTotal.Load())})
 }
 s.mu.RUnlock()
 return out
}

func maxDuration(v,minimum time.Duration) time.Duration {
 if v<minimum { return minimum }
 return v
}
