package collector

import (
	"testing"
	"time"
)

func TestLatestStoreBoundedAndRefreshesValues(t *testing.T) {
	store:=NewLatestStore(2)
	now:=time.Now().UTC()
	store.Update([]Sample{
		{DeviceID:"d1",PointCode:"p1",Value:float64(1),Quality:QualityGood,Timestamp:now},
		{DeviceID:"d1",PointCode:"p2",Value:float64(2),Quality:QualityGood,Timestamp:now},
	})
	store.Update([]Sample{{DeviceID:"d1",PointCode:"p1",Value:float64(3),Quality:QualityGood,Timestamp:now.Add(time.Second)}})
	if got,ok:=store.Get("d1","p1");!ok||got.Value!=float64(3){t.Fatalf("updated sample=%+v exists=%v",got,ok)}
	lastSuccess:=now.Add(time.Second)
	store.Update([]Sample{{DeviceID:"d1",PointCode:"p1",Quality:QualityCommError,Error:"timeout",Timestamp:now.Add(2*time.Second)}})
	if got,_:=store.Get("d1","p1");!got.LastSuccess.Equal(lastSuccess)||got.Quality!=QualityCommError||got.Value!=float64(3){t.Fatalf("communication error lost last success: %+v",got)}
	store.Update([]Sample{{DeviceID:"d2",PointCode:"p3",Value:float64(4),Quality:QualityGood,Timestamp:now.Add(2*time.Second)}})
	if _,ok:=store.Get("d1","p2");ok{t.Fatal("oldest point was not evicted")}
	if got:=store.Snapshot("d2");len(got)!=1||got["p3"].Value!=float64(4){t.Fatalf("device snapshot=%+v",got)}
}

func TestLatestStoreMarksStaleWithoutLosingLastValue(t *testing.T) {
 store:=NewLatestStore(10)
 lastGood:=time.Now().UTC().Add(-3*time.Minute)
 store.Update([]Sample{{DeviceID:"ups-1",PointCode:"battery_voltage",Value:float64(220.5),Quality:QualityGood,Timestamp:lastGood}})
 store.Update([]Sample{{DeviceID:"ups-1",PointCode:"battery_voltage",Quality:QualityCommError,Error:"timeout",Timestamp:lastGood.Add(time.Minute)}})
 key:=PointIntervalKey("ups-1","battery_voltage")
 changed:=store.MarkStale(lastGood.Add(4*time.Minute),map[string]time.Duration{key:time.Minute},2)
 if changed!=1 { t.Fatalf("stale transitions=%d",changed) }
 got,ok:=store.Get("ups-1","battery_voltage")
 if !ok { t.Fatal("missing latest point") }
 if got.Quality!=QualityStale || got.QualityReason!="STALE_TIMEOUT" { t.Fatalf("unexpected quality: %+v",got) }
 if got.Value!=float64(220.5) || !got.LastSuccess.Equal(lastGood) { t.Fatalf("last known value lost: %+v",got) }
 if again:=store.MarkStale(lastGood.Add(5*time.Minute),map[string]time.Duration{key:time.Minute},2);again!=0 { t.Fatalf("stale transition repeated: %d",again) }
}

func TestLatestStoreBadDecodePreservesLastValue(t *testing.T) {
 store:=NewLatestStore(10)
 now:=time.Now().UTC()
 store.Update([]Sample{{DeviceID:"meter-1",PointCode:"voltage",Value:float64(230),Quality:QualityGood,Timestamp:now}})
 store.Update([]Sample{{DeviceID:"meter-1",PointCode:"voltage",Quality:QualityBad,Error:"invalid BCD",Timestamp:now.Add(time.Second)}})
 got,_:=store.Get("meter-1","voltage")
 if got.Value!=float64(230) || got.Quality!=QualityBad { t.Fatalf("bad sample overwrote last valid value: %+v",got) }
}

func TestLatestStoreQualityCounts(t *testing.T) {
 store:=NewLatestStore(10)
 now:=time.Now().UTC()
 store.Update([]Sample{
  {DeviceID:"d1",PointCode:"good",Value:1,Quality:QualityGood,Timestamp:now},
  {DeviceID:"d1",PointCode:"bad",Quality:QualityBad,Error:"decode",Timestamp:now},
  {DeviceID:"d1",PointCode:"uncertain",Value:2,Quality:QualityUncertain,Timestamp:now},
 })
 counts:=store.QualityCounts()
 if counts[QualityGood]!=1 || counts[QualityBad]!=1 || counts[QualityUncertain]!=1 {
  t.Fatalf("unexpected quality counts: %+v",counts)
 }
}
