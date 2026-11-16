package pipeline

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/reliability/wal"
)

func TestPersistResultsWritesSamplesToWAL(t *testing.T) {
	store, err := wal.Open(wal.Options{Dir: t.TempDir(), MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	results := make(chan collector.Result, 1)
	results <- collector.Result{
		DeviceID: "device-1",
		At: time.Now().UTC(),
		Samples: []collector.Sample{{
			EventID: "device-1/voltage/1",
			DeviceID: "device-1",
			Model: "METER_A",
			PointCode: "voltage",
			Timestamp: time.Now().UTC(),
			Value: 220.5,
			Quality: collector.QualityGood,
		}},
	}
	close(results)

	if err := PersistResults(context.Background(), results, store); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].EventID != "device-1/voltage/1" {
		t.Fatalf("pending records = %+v", pending)
	}
	var got collector.Sample
	if err := json.Unmarshal(pending[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.DeviceID != "device-1" || got.PointCode != "voltage" || got.Value != 220.5 {
		t.Fatalf("unexpected persisted sample: %+v", got)
	}
}
