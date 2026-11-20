package telemetry

import (
 "math"
 "sync"
 "testing"
 "time"
)

func TestNormalizeProtocol(t *testing.T) {
 got,err:=NormalizeProtocol(" DL/T645-2007 ")
 if err!=nil || got!=ProtocolDLT645 { t.Fatalf("got %q, err %v",got,err) }
 if _,err:=NormalizeProtocol("unknown"); err==nil { t.Fatal("expected unsupported protocol error") }
}

func TestSampleValidation(t *testing.T) {
 base:=Sample{DeviceID:"ups-1",PointCode:"battery_voltage",Protocol:ProtocolModbusTCP,CollectedAt:time.Now(),Value:220.5,Quality:QualityGood}
 if err:=base.Validate(); err!=nil { t.Fatal(err) }
 base.Value=math.NaN()
 if err:=base.Validate(); err==nil { t.Fatal("NaN must not be GOOD") }
 base.Value=220.5
 base.Quality=QualityBad
 if err:=base.Validate(); err==nil { t.Fatal("BAD requires reason") }
 base.Reason=ReasonTimeout
 if err:=base.Validate(); err!=nil { t.Fatal(err) }
}

func TestEvaluateStalenessPreservesValue(t *testing.T) {
 now:=time.Now().UTC()
 s:=Sample{DeviceID:"meter-1",PointCode:"voltage",Protocol:ProtocolDLT645,Value:230.1,Quality:QualityGood,CollectedAt:now.Add(-3*time.Minute)}
 got,changed:=EvaluateStaleness(s,now,time.Minute,2)
 if !changed || got.Quality!=QualityStale || got.Reason!=ReasonStaleTimeout { t.Fatalf("unexpected stale result: %+v",got) }
 if got.Value!=230.1 || !got.CollectedAt.Equal(s.CollectedAt) { t.Fatal("stale evaluation must preserve last value and timestamp") }
}

func TestCommunicationStatsConcurrent(t *testing.T) {
 stats:=NewCommunicationStats()
 var wg sync.WaitGroup
 for i:=0;i<20;i++ {
  wg.Add(1)
  go func() {
   defer wg.Done()
   for j:=0;j<100;j++ { stats.Record("ups-1",time.Now(),20*time.Millisecond,"",false,false) }
  }()
 }
 wg.Wait()
 snapshot,ok:=stats.Snapshot("ups-1")
 if !ok { t.Fatal("missing device snapshot") }
 if snapshot.Attempts!=2000 || snapshot.Successes!=2000 || snapshot.Failures!=0 { t.Fatalf("bad counters: %+v",snapshot) }
 if snapshot.LatencyCount!=snapshot.Attempts { t.Fatalf("latency count mismatch: %+v",snapshot) }
}

func TestProtocolCommunicationCounters(t *testing.T) {
 stats:=NewCommunicationStats()
 stats.RecordProtocol(string(ProtocolDLT645),"meter-1",time.Now(),150*time.Millisecond,"TIMEOUT",true,false)
 snapshots:=stats.ProtocolSnapshots()
 if len(snapshots)!=1 { t.Fatalf("protocol snapshots=%d",len(snapshots)) }
 got:=snapshots[0]
 if got.Protocol!=string(ProtocolDLT645)||got.Attempts!=1||got.Failures!=1||got.Timeouts!=1 { t.Fatalf("unexpected protocol counters: %+v",got) }
}
