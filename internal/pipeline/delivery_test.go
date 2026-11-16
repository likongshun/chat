package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/likongshun/chat/internal/reliability/wal"
)

type testSink struct {
	name      string
	calls     atomic.Int32
	failFirst bool
}

func (s *testSink) Name() string { return s.name }
func (s *testSink) Publish(_ context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	call := s.calls.Add(1)
	if s.failFirst && call == 1 {
		return errors.New("temporary sink outage")
	}
	return nil
}

func TestDeliveryWorkerRetriesOnlyUnackedSink(t *testing.T) {
	store, err := wal.Open(wal.Options{Dir: t.TempDir(), MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append("delivery-event", []byte(`{"device_id":"d1","point_code":"p1"}`)); err != nil {
		t.Fatal(err)
	}

	mqttSink := &testSink{name: "mqtt"}
	influxSink := &testSink{name: "influx", failFirst: true}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &DeliveryWorker{
		Store: store,
		Sinks: []Sink{mqttSink, influxSink},
		BatchSize: 10,
		PollInterval: 10 * time.Millisecond,
		RetryInterval: 10 * time.Millisecond,
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := store.Pending()
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 && mqttSink.calls.Load() >= 1 && influxSink.calls.Load() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	pending, err := store.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("delivery did not finish; pending=%d", len(pending))
	}
	if mqttSink.calls.Load() != 1 {
		t.Fatalf("MQTT calls=%d, want 1 (must not republish ACKed sink)", mqttSink.calls.Load())
	}
	if influxSink.calls.Load() != 2 {
		t.Fatalf("Influx calls=%d, want 2 (one failure and one retry)", influxSink.calls.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery worker did not stop")
	}
}
