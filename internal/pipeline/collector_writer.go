package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/reliability/wal"
)

const (
	maxWALBatchRecords = 1000
	walBatchInterval = 10 * time.Millisecond
)

// PersistResults batches point samples to amortize fsync cost while preserving
// bounded memory use. A sample is handed off only after AppendBatch succeeds.
func PersistResults(results <-chan collector.Result, store *wal.WAL) error {
	if store == nil {
		return fmt.Errorf("WAL store is required")
	}
	ticker := time.NewTicker(walBatchInterval)
	defer ticker.Stop()

	pending := make([]wal.AppendInput, 0, maxWALBatchRecords)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		_, err := store.AppendBatch(pending)
		if err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	for {
		select {
		case result, ok := <-results:
			if !ok {
				return flush()
			}
			for _, sample := range result.Samples {
				payload, err := json.Marshal(sample)
				if err != nil {
					return fmt.Errorf("marshal sample %s/%s: %w", sample.DeviceID, sample.PointCode, err)
				}
				pending = append(pending, wal.AppendInput{EventID: sample.EventID, Payload: payload})
				if len(pending) >= maxWALBatchRecords {
					if err := flush(); err != nil {
						return fmt.Errorf("flush WAL sample batch: %w", err)
					}
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return fmt.Errorf("flush WAL sample batch: %w", err)
			}
		}
	}
}
