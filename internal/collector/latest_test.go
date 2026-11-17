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
	store.Update([]Sample{{DeviceID:"d2",PointCode:"p3",Value:float64(4),Quality:QualityGood,Timestamp:now.Add(2*time.Second)}})
	if _,ok:=store.Get("d1","p2");ok{t.Fatal("oldest point was not evicted")}
	if got:=store.Snapshot("d2");len(got)!=1||got["p3"].Value!=float64(4){t.Fatalf("device snapshot=%+v",got)}
}
