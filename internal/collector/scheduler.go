package collector

import (
 "context"
 "sync"
 "time"
 "github.com/likongshun/chat/internal/config"
 "github.com/likongshun/chat/internal/modbus"
)
type Result struct { DeviceID string; At time.Time; Err error }
type Scheduler struct { Workers int; Timeout time.Duration }
func (s Scheduler) Poll(ctx context.Context,devices []config.Device,fn func(context.Context,config.Device)error)[]Result{
 n:=s.Workers;if n<1{n=1};jobs:=make(chan config.Device);out:=make(chan Result,len(devices));var wg sync.WaitGroup
 for i:=0;i<n;i++{wg.Add(1);go func(){defer wg.Done();for d:=range jobs{c,cancel:=context.WithTimeout(ctx,s.Timeout);e:=fn(c,d);cancel();out<-Result{DeviceID:d.ID,At:time.Now(),Err:e}}}()}
 go func(){defer close(jobs);for _,d:=range devices{if d.Enabled{select{case jobs<-d:case <-ctx.Done():return}}}}()
 wg.Wait();close(out);var results []Result;for r:=range out{results=append(results,r)};return results
}
var _ = modbus.Client{}
