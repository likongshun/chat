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
type Manifest struct { Version int `json:"version"`; CheckpointSeq uint64 `json:"checkpoint_seq"`; CheckpointFile string `json:"checkpoint_file,omitempty"`; CheckpointChecksum string `json:"checkpoint_checksum,omitempty"`; FirstSegment uint64 `json:"first_segment"`; ActiveSegment uint64 `json:"active_segment"`; AckFirstSegment uint64 `json:"ack_first_segment,omitempty"`; AckActiveSegment uint64 `json:"ack_active_segment,omitempty"`; Checksum string `json:"checksum,omitempty"` }
type Checkpoint struct { FormatVersion int `json:"format_version,omitempty"`; Seq uint64 `json:"seq"`; CreatedAt time.Time `json:"created_at"`; Pending []PendingRecord `json:"pending,omitempty"` }

type PendingRecord struct {
 Record
 MQTTAcked bool `json:"mqtt_acked"`
 InfluxAcked bool `json:"influx_acked"`
}

type AppendInput struct {
 EventID string
 Payload json.RawMessage
}

type Stats struct {
 DiskBytes int64
 MaxBytes int64
 DiskUsedRatio float64
 FilesystemBytes uint64
 FilesystemTotalBytes uint64
 FilesystemUsedRatio float64
 DiskLevel string
 PendingRecords int
 CapacityRejections uint64
 AdmissionLimitBytes int64
 OldestPendingAge time.Duration
 SegmentCount int
 AckSegmentCount int
 CheckpointSeq uint64
 FirstSegment uint64
 ActiveSegment uint64
 Fsync FsyncStats
}
