package pipeline

import (
	"context"
	"encoding/json"
	"errors"
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
func PersistResults(ctx context.Context, results <-chan collector.Result, store *wal.WAL) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
		for {
			_, err := store.AppendBatch(pending)
			if err == nil {
				pending = pending[:0]
				return nil
			}
			if !errors.Is(err, wal.ErrCapacity) {
				return err
			}
			// Keep the bounded batch in memory while the delivery worker ACKs
			// records and reclaims old segments to make room.
			if !waitContext(ctx, 250*time.Millisecond) {
				return ctx.Err()
			}
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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
