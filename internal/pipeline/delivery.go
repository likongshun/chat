package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/likongshun/chat/internal/reliability/wal"
)

type DeliveryStore interface {
	PendingWithAcks() ([]wal.PendingRecord, error)
	Ack(wal.Ack) error
	CheckpointReady() (uint64, error)
}

type DeliveryWorker struct {
	Store         DeliveryStore
	Sinks         []Sink
	BatchSize     int
	PollInterval  time.Duration
	RetryInterval time.Duration
	OnError       func(error)
	Metrics       *DeliveryMetrics
}

func (w *DeliveryWorker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if w.Store == nil {
		return fmt.Errorf("delivery store is required")
	}
	sinks := make(map[string]Sink, len(w.Sinks))
	for _, sink := range w.Sinks {
		if sink == nil {
			return fmt.Errorf("nil delivery sink")
		}
		name := sink.Name()
		if name != "mqtt" && name != "influx" {
			return fmt.Errorf("unsupported delivery sink %q", name)
		}
		if sinks[name] != nil {
			return fmt.Errorf("duplicate delivery sink %q", name)
		}
		sinks[name] = sink
	}
	if sinks["mqtt"] == nil || sinks["influx"] == nil {
		return fmt.Errorf("both mqtt and influx sinks are required")
	}
	batchSize := w.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	pollInterval := w.PollInterval
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	retryInterval := w.RetryInterval
	if retryInterval <= 0 {
		retryInterval = time.Second
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		pending, err := w.Store.PendingWithAcks()
		if err != nil {
			return fmt.Errorf("read WAL pending records: %w", err)
		}
		hadError := false
		for _, name := range []string{"mqtt", "influx"} {
			sink := sinks[name]
			events := make([]Event, 0, len(pending))
			for _, record := range pending {
				acked := record.MQTTAcked
				if name == "influx" {
					acked = record.InfluxAcked
				}
				if !acked {
					events = append(events, Event{Seq: record.Seq, ID: record.EventID, Payload: record.Payload})
				}
			}
			for start := 0; start < len(events); start += batchSize {
				end := start + batchSize
				if end > len(events) {
					end = len(events)
				}
				batch := events[start:end]
				if err := sink.Publish(ctx, batch); err != nil {
					w.metricsFor(name).publishErrors.Add(1)
					w.report(fmt.Errorf("publish %s batch: %w", name, err))
					hadError = true
					break
				}
				w.metricsFor(name).published.Add(uint64(len(batch)))
				for _, event := range batch {
					if err := w.Store.Ack(wal.Ack{Seq: event.Seq, Sink: name, EventID: event.ID}); err != nil {
						w.metricsFor(name).ackErrors.Add(1)
						w.report(fmt.Errorf("persist %s ACK for seq %d: %w", name, event.Seq, err))
						hadError = true
						break
					}
				}
			}
		}
		if _, err := w.Store.CheckpointReady(); err != nil {
			w.report(fmt.Errorf("advance WAL checkpoint: %w", err))
			hadError = true
		}
		if hadError || len(pending) == 0 {
			delay := pollInterval
			if hadError {
				delay = retryInterval
			}
			if !waitContext(ctx, delay) {
				return nil
			}
		}
	}
}

func (w *DeliveryWorker) report(err error) {
	if w.OnError != nil {
		w.OnError(err)
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}



// DeliveryMetrics stores bounded-cardinality counters for the two configured sinks.
type DeliveryMetrics struct {
 mqtt atomicSinkMetrics
 influx atomicSinkMetrics
}
type atomicSinkMetrics struct {
 published atomic.Uint64
 publishErrors atomic.Uint64
 ackErrors atomic.Uint64
}
type SinkMetricsSnapshot struct {
 Published uint64
 PublishErrors uint64
 AckErrors uint64
}
type DeliveryMetricsSnapshot struct {
 MQTT SinkMetricsSnapshot
 Influx SinkMetricsSnapshot
}
func (m *DeliveryMetrics) Snapshot() DeliveryMetricsSnapshot {
 if m==nil{return DeliveryMetricsSnapshot{}}
 return DeliveryMetricsSnapshot{
  MQTT:SinkMetricsSnapshot{Published:m.mqtt.published.Load(),PublishErrors:m.mqtt.publishErrors.Load(),AckErrors:m.mqtt.ackErrors.Load()},
  Influx:SinkMetricsSnapshot{Published:m.influx.published.Load(),PublishErrors:m.influx.publishErrors.Load(),AckErrors:m.influx.ackErrors.Load()},
 }
}
func (w *DeliveryWorker) metricsFor(name string)*atomicSinkMetrics {
 if w.Metrics==nil{w.Metrics=&DeliveryMetrics{}}
 if name=="mqtt"{return &w.Metrics.mqtt}
 return &w.Metrics.influx
}
