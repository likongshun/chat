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
}

type LatestSample struct {
	DeviceID string    `json:"device_id"`
	PointCode string   `json:"point_code"`
	Value any          `json:"value,omitempty"`
	Quality Quality    `json:"quality"`
	Error string       `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

func NewLatestStore(maxEntries int) *LatestStore {
	if maxEntries <= 0 { maxEntries = 100000 }
	return &LatestStore{max:maxEntries, items:make(map[string]LatestSample), order:list.New(), index:make(map[string]*list.Element)}
}

func latestKey(deviceID, pointCode string) string { return deviceID + "\x00" + pointCode }

func (s *LatestStore) Update(samples []Sample) {
	if s == nil || len(samples)==0 { return }
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sample := range samples {
		key:=latestKey(sample.DeviceID,sample.PointCode)
		if element:=s.index[key]; element!=nil {
			s.items[key]=LatestSample{DeviceID:sample.DeviceID,PointCode:sample.PointCode,Value:sample.Value,Quality:sample.Quality,Error:sample.Error,Timestamp:sample.Timestamp}
			s.order.MoveToBack(element)
			continue
		}
		for len(s.items)>=s.max {
			oldest:=s.order.Front()
			if oldest==nil { break }
			oldKey:=oldest.Value.(string)
			delete(s.items,oldKey);delete(s.index,oldKey);s.order.Remove(oldest)
		}
		s.items[key]=LatestSample{DeviceID:sample.DeviceID,PointCode:sample.PointCode,Value:sample.Value,Quality:sample.Quality,Error:sample.Error,Timestamp:sample.Timestamp}
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
	for _,v:=range s.items { if deviceID==""||v.DeviceID==deviceID { out[v.PointCode]=v } }
	return out
}
