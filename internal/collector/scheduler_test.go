package collector

import (\n\t"math/rand"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/likongshun/chat/internal/config"
)

func TestSchedulerRunSerializesPerDeviceAndBoundsConcurrency(t *testing.T) {
	devices:=[]config.Device{
		{ID:"d1",Interval:10*time.Millisecond,Enabled:true},
		{ID:"d2",Interval:10*time.Millisecond,Enabled:true},
		{ID:"disabled",Interval:10*time.Millisecond,Enabled:false},
	}
	var active,maxActive atomic.Int32
	var d1Active,d1Overlap atomic.Int32
	poll:=func(ctx context.Context,d config.Device)([]Sample,error){
		if d.ID=="d1" && d1Active.Add(1)>1 { d1Overlap.Add(1) }
		defer func(){if d.ID=="d1"{d1Active.Add(-1)}}()
		n:=active.Add(1)
		for {
			old:=maxActive.Load()
			if n<=old || maxActive.CompareAndSwap(old,n) {break}
		}
		defer active.Add(-1)
		select {case <-time.After(3*time.Millisecond): case <-ctx.Done(): return nil,ctx.Err()}
		return []Sample{{DeviceID:d.ID,PointCode:"p",Timestamp:time.Now(),Value:1,Quality:QualityGood}},nil
	}
	ctx,cancel:=context.WithCancel(context.Background())
	s:=Scheduler{Workers:2,Timeout:100*time.Millisecond,OutputBuffer:4}
	results:=s.Run(ctx,devices,poll)
	count:=0
	for r:=range results {
		if r.DeviceID=="disabled" {t.Fatal("disabled device was polled")}
		count++
		if count==8 {cancel()}
	}
	if count<2 {t.Fatalf("expected polling results, got %d",count)}
	if got:=maxActive.Load(); got>2 {t.Fatalf("global concurrency exceeded: %d",got)}
	if got:=d1Overlap.Load(); got!=0 {t.Fatalf("same device polled concurrently: %d",got)}
}

func TestSchedulerTimeoutAndMetrics(t *testing.T) {
	devices:=[]config.Device{{ID:"d1",Interval:time.Second,Enabled:true}}
	metrics:=&Metrics{}
	ctx,cancel:=context.WithTimeout(context.Background(),40*time.Millisecond)
	defer cancel()
	s:=Scheduler{Workers:1,Timeout:10*time.Millisecond,OutputBuffer:2,Metrics:metrics}
	poll:=func(ctx context.Context,d config.Device)([]Sample,error){
		<-ctx.Done()
		return nil,ctx.Err()
	}
	for range s.Run(ctx,devices,poll) {}
	snap:=metrics.Snapshot()
	if snap.PollsTotal==0 || snap.PollErrorsTotal==0 || snap.PollTimeoutsTotal==0 {
		t.Fatalf("unexpected metrics: %+v",snap)
	}
}

func TestBackoffIsBounded(t *testing.T) {
	r:=rand.New(rand.NewSource(1))
	for failures:=1;failures<20;failures++ {
		got:=backoff(100*time.Millisecond,time.Second,failures,r)
		if got<80*time.Millisecond || got>1200*time.Millisecond {t.Fatalf("backoff out of bounds: %s",got)}
	}
}
