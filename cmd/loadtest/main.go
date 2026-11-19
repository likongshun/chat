package main

import (
 "context"
 "encoding/json"
 "errors"
 "flag"
 "fmt"
 "os"
 "runtime"
 "sync/atomic"
 "time"

 "github.com/likongshun/chat/internal/collector"
 "github.com/likongshun/chat/internal/config"
 "github.com/likongshun/chat/internal/telemetry"
)

type simulatedPoller struct {
 protocol string
 points int
 latency time.Duration
 errorEvery uint64
 sequence atomic.Uint64
}

func (p *simulatedPoller) Protocol() string { return p.protocol }
func (p *simulatedPoller) Close() error { return nil }

func (p *simulatedPoller) Poll(ctx context.Context,d config.Device)([]collector.Sample,error) {
 if p.latency>0 {
  timer:=time.NewTimer(p.latency)
  defer timer.Stop()
  select { case <-ctx.Done(): return nil,ctx.Err(); case <-timer.C: }
 }
 seq:=p.sequence.Add(1)
 if p.errorEvery>0 && seq%p.errorEvery==0 {
  return nil,fmt.Errorf("simulated %s timeout",p.protocol)
 }
 now:=time.Now().UTC()
 samples:=make([]collector.Sample,0,p.points)
 for i:=0;i<p.points;i++ {
  code:=fmt.Sprintf("point_%04d",i+1)
  value:=float64((seq+uint64(i))%100000)/10
  samples=append(samples,collector.Sample{
   EventID:fmt.Sprintf("%s/%s/%d",d.ID,code,now.UnixNano()),
   DeviceID:d.ID,Protocol:p.protocol,Model:d.Model,PointCode:code,
   ConfigVersion:1,Timestamp:now,Value:value,Quality:collector.QualityGood,
  })
 }
 return samples,nil
}

type protocolSummary struct {
 Attempts uint64 `json:"attempts"`
 Successes uint64 `json:"successes"`
 Failures uint64 `json:"failures"`
 Timeouts uint64 `json:"timeouts"`
}

type summary struct {
 Profile string `json:"profile"`
 Devices int `json:"devices"`
 PointsPerDevice int `json:"points_per_device"`
 Workers int `json:"workers"`
 PollInterval string `json:"poll_interval"`
 SimulatedLatency string `json:"simulated_latency"`
 RequestedDuration string `json:"requested_duration"`
 ActualDuration string `json:"actual_duration"`
 Polls uint64 `json:"polls"`
 PollErrors uint64 `json:"poll_errors"`
 Samples uint64 `json:"samples"`
 PollsPerSecond float64 `json:"polls_per_second"`
 SamplesPerSecond float64 `json:"samples_per_second"`
 AveragePollLatencyMS float64 `json:"average_poll_latency_ms"`
 MaxPollLatencyMS float64 `json:"max_poll_latency_ms"`
 HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
 Goroutines int `json:"goroutines"`
 Protocols map[string]protocolSummary `json:"protocols"`
 Quality map[string]uint64 `json:"quality"`
}

func main() {
 devicesN:=flag.Int("devices",100,"number of simulated devices")
 pointsN:=flag.Int("points",20,"points emitted per successful poll")
 workers:=flag.Int("workers",100,"maximum concurrent device polls")
 interval:=flag.Duration("interval",time.Second,"poll interval per device")
 duration:=flag.Duration("duration",30*time.Second,"test duration")
 latency:=flag.Duration("latency",2*time.Millisecond,"simulated protocol response latency")
 dltRatio:=flag.Float64("dlt-ratio",0.25,"fraction of devices using DL/T645; remainder uses Modbus TCP")
 errorEvery:=flag.Uint64("error-every",0,"inject one deterministic timeout every N polls per protocol; 0 disables errors")
 flag.Parse()

 if *devicesN<1||*devicesN>5000||*pointsN<1||*pointsN>1000||*workers<1||*duration<=0||*interval<=0||*latency<0||*dltRatio<0||*dltRatio>1 {
  fmt.Fprintln(os.Stderr,"invalid arguments: devices 1..5000, points 1..1000, workers >=1, positive duration/interval, nonnegative latency, dlt-ratio 0..1")
  os.Exit(2)
 }
 dltCount:=int(float64(*devicesN)*(*dltRatio))
 modbus:=&simulatedPoller{protocol:"modbus-tcp",points:*pointsN,latency:*latency,errorEvery:*errorEvery}
 dlt:=&simulatedPoller{protocol:"dl-t645-2007",points:*pointsN,latency:*latency,errorEvery:*errorEvery}
 router:=&collector.ProtocolRouter{Engines:map[string]collector.ProtocolPoller{
  "modbus-tcp":modbus,"dl-t645-2007":dlt,
 }}
 devices:=make([]config.Device,0,*devicesN)
 for i:=0;i<*devicesN;i++ {
  protocol:="modbus-tcp"
  if i<dltCount { protocol="dl-t645-2007" }
  devices=append(devices,config.Device{
   ID:fmt.Sprintf("sim-%05d",i+1),Protocol:protocol,Model:"simulated",
   Interval:*interval,Enabled:true,
  })
 }
 metrics:=&collector.Metrics{}
 comm:=telemetry.NewCommunicationStats()
 latest:=collector.NewLatestStore(*devicesN**pointsN+1)
 engine:=&collector.Engine{Scheduler:collector.Scheduler{
  Workers:*workers,Timeout:maxDuration(*latency*4,100*time.Millisecond),
  OutputBuffer:maxInt(*devicesN*2,1000),Metrics:metrics,DeviceStats:comm,
 },Router:router}

 ctx,cancel:=context.WithTimeout(context.Background(),*duration)
 defer cancel()
 started:=time.Now()
 var polls,pollErrors,samples uint64
 for result:=range engine.Run(ctx,devices) {
  polls++
  if result.Err!=nil { pollErrors++ }
  samples+=uint64(len(result.Samples))
  latest.Update(result.Samples)
 }
 actual:=time.Since(started)
 _=engine.Close()
 metric:=metrics.Snapshot()
 fleet:=comm.FleetSnapshot()
 var mem runtime.MemStats
 runtime.ReadMemStats(&mem)
 protocolStats:=make(map[string]protocolSummary)
 for _,item:=range comm.ProtocolSnapshots() {
  protocolStats[item.Protocol]=protocolSummary{Attempts:item.Attempts,Successes:item.Successes,Failures:item.Failures,Timeouts:item.Timeouts}
 }
 quality:=make(map[string]uint64)
 for k,v:=range latest.QualityCounts() { quality[string(k)]=v }
 if actual<=0 { actual=time.Nanosecond }
 out:=summary{
  Profile:"mixed-protocol-scheduler-simulation",Devices:*devicesN,PointsPerDevice:*pointsN,
  Workers:*workers,PollInterval:interval.String(),SimulatedLatency:latency.String(),
  RequestedDuration:duration.String(),ActualDuration:actual.Round(time.Millisecond).String(),
  Polls:polls,PollErrors:pollErrors,Samples:samples,
  PollsPerSecond:float64(polls)/actual.Seconds(),SamplesPerSecond:float64(samples)/actual.Seconds(),
  AveragePollLatencyMS:durationMS(fleet.LatencyTotal,fleet.LatencyCount),
  MaxPollLatencyMS:float64(metric.MaxDuration)/float64(time.Millisecond),
  HeapAllocBytes:mem.HeapAlloc,Goroutines:runtime.NumGoroutine(),
  Protocols:protocolStats,Quality:quality,
 }
 enc:=json.NewEncoder(os.Stdout);enc.SetIndent("","  ")
 if err:=enc.Encode(out);err!=nil&&!errors.Is(err,context.Canceled) { fmt.Fprintln(os.Stderr,err);os.Exit(1) }
}

func durationMS(total time.Duration,count uint64) float64 {
 if count==0 { return 0 }
 return float64(total)/float64(count)/float64(time.Millisecond)
}
func maxDuration(a,b time.Duration) time.Duration { if a>b{return a};return b }
func maxInt(a,b int) int { if a>b{return a};return b }
