package wal

import (
 "encoding/json"
 "errors"
 "time"
)

var ErrCapacity = errors.New("WAL capacity limit reached")
type Options struct { Dir string; SegmentBytes int64; MaxBytes int64; ReserveBytes int64; Sync bool }
type Record struct { Seq uint64 `json:"seq"`; EventID string `json:"event_id"`; Payload json.RawMessage `json:"payload"`; CreatedAt time.Time `json:"created_at"` }
type Ack struct { Seq uint64 `json:"seq"`; Sink string `json:"sink"`; EventID string `json:"event_id"` }
type Manifest struct { Version int `json:"version"`; CheckpointSeq uint64 `json:"checkpoint_seq"`; FirstSegment uint64 `json:"first_segment"`; ActiveSegment uint64 `json:"active_segment"`; Checksum string `json:"checksum,omitempty"` }
type Checkpoint struct { Seq uint64 `json:"seq"`; CreatedAt time.Time `json:"created_at"` }

type PendingRecord struct {
 Record
 MQTTAcked bool
 InfluxAcked bool
}

type AppendInput struct {
 EventID string
 Payload json.RawMessage
}

type Stats struct {
 DiskBytes int64
 MaxBytes int64
 DiskUsedRatio float64
 PendingRecords int
 CapacityRejections uint64
 AdmissionLimitBytes int64
 OldestPendingAge time.Duration
 SegmentCount int
 CheckpointSeq uint64
 FirstSegment uint64
 ActiveSegment uint64
}
