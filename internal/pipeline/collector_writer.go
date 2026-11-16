package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/reliability/wal"
)

// PersistResults durably records every point sample emitted by the scheduler.
// The WAL append sync boundary is the handoff point: a sample is not considered
// persisted until Append returns successfully.
func PersistResults(results <-chan collector.Result, store *wal.WAL) error {
	if store == nil {
		return fmt.Errorf("WAL store is required")
	}
	for result := range results {
		for _, sample := range result.Samples {
			payload, err := json.Marshal(sample)
			if err != nil {
				return fmt.Errorf("marshal sample %s/%s: %w", sample.DeviceID, sample.PointCode, err)
			}
			if _, err = store.Append(sample.EventID, payload); err != nil {
				return fmt.Errorf("append sample %s/%s to WAL: %w", sample.DeviceID, sample.PointCode, err)
			}
		}
	}
	return nil
}
