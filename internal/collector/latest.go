package collector

import (
	"container/list"
	"sync"
	"time"
)

// LatestStore is a bounded in-memory view of the most recently observed point
// values. It is deliberately not a historical store; InfluxDB remains the
// source of truth for time-range queries.
type LatestStore struct {
	mu sync.RWMutex
	max int
	items map[string]LatestSample
	order *list.List
	index map[string]*list.Element
	byDevice map[string]map[string]LatestSample
}

type LatestSample struct {
	DeviceID string    `json:"device_id"`
	PointCode string   `json:"point_code"`
	Value any          `json:"value,omitempty"`
	Quality Quality    `json:"quality"`
	Error string       `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	LastSuccess time.Time `json:"last_success,omitempty"`
}

func NewLatestStore(maxEntries int) *LatestStore {
	if maxEntries <= 0 { maxEntries = 100000 }
	return &LatestStore{max:maxEntries, items:make(map[string]LatestSample), order:list.New(), index:make(map[string]*list.Element), byDevice:make(map[string]map[string]LatestSample)}
}

func latestKey(deviceID, pointCode string) string { return deviceID + "\x00" + pointCode }

func (s *LatestStore) Update(samples []Sample) {
	if s == nil || len(samples)==0 { return }
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sample := range samples {
		key:=latestKey(sample.DeviceID,sample.PointCode)
		if element:=s.index[key]; element!=nil {
			previous:=s.items[key]
			lastSuccess:=previous.LastSuccess
			if sample.Quality!=QualityCommError { lastSuccess=sample.Timestamp }
			latest:=LatestSample{DeviceID:sample.DeviceID,PointCode:sample.PointCode,Value:sample.Value,Quality:sample.Quality,Error:sample.Error,Timestamp:sample.Timestamp,LastSuccess:lastSuccess}
			s.items[key]=latest
			s.byDevice[sample.DeviceID][sample.PointCode]=latest
			s.order.MoveToBack(element)
			continue
		}
		for len(s.items)>=s.max {
			oldest:=s.order.Front()
			if oldest==nil { break }
			oldKey:=oldest.Value.(string)
			oldSample:=s.items[oldKey]
			delete(s.items,oldKey);delete(s.index,oldKey)
			delete(s.byDevice[oldSample.DeviceID],oldSample.PointCode)
			if len(s.byDevice[oldSample.DeviceID])==0 { delete(s.byDevice,oldSample.DeviceID) }
			s.order.Remove(oldest)
		}
		lastSuccess:=time.Time{}
		if sample.Quality!=QualityCommError { lastSuccess=sample.Timestamp }
		latest:=LatestSample{DeviceID:sample.DeviceID,PointCode:sample.PointCode,Value:sample.Value,Quality:sample.Quality,Error:sample.Error,Timestamp:sample.Timestamp,LastSuccess:lastSuccess}
		s.items[key]=latest
		if s.byDevice[sample.DeviceID]==nil { s.byDevice[sample.DeviceID]=make(map[string]LatestSample) }
		s.byDevice[sample.DeviceID][sample.PointCode]=latest
		s.index[key]=s.order.PushBack(key)
	}
}

func (s *LatestStore) Get(deviceID, pointCode string) (LatestSample,bool) {
	if s==nil{return LatestSample{},false}
	s.mu.RLock();defer s.mu.RUnlock()
	v,ok:=s.items[latestKey(deviceID,pointCode)]
	return v,ok
}

func (s *LatestStore) Snapshot(deviceID string) map[string]LatestSample {
	out:=make(map[string]LatestSample)
	if s==nil{return out}
	s.mu.RLock();defer s.mu.RUnlock()
	if deviceID!="" { for code,v:=range s.byDevice[deviceID] { out[code]=v }; return out }
	for _,v:=range s.items { out[v.PointCode]=v }
	return out
}

func (s *LatestStore) LastSuccess(deviceID string) time.Time {
	if s==nil{return time.Time{}}
	s.mu.RLock();defer s.mu.RUnlock()
	var latest time.Time
	for _,sample:=range s.byDevice[deviceID] { if sample.LastSuccess.After(latest) { latest=sample.LastSuccess } }
	return latest
}
