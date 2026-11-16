package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/pipeline"
	"github.com/likongshun/chat/internal/reliability/wal"
)

type Server struct {
	Addr      string
	Collector *collector.Metrics
	WAL       *wal.WAL
	Delivery  *pipeline.DeliveryMetrics
}

func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	addr := s.Addr
	if addr == "" {
		addr = ":9090"
	}
	server := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) Handler() http.Handler {
 mux := http.NewServeMux()
 mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
  w.WriteHeader(http.StatusOK)
  _, _ = w.Write([]byte("ok\n"))
 })
 mux.HandleFunc("/readyz", s.ready)
 mux.HandleFunc("/metrics", s.metrics)
 return mux
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	if s.WAL == nil {
		http.Error(w, "WAL unavailable", http.StatusServiceUnavailable)
		return
	}
	stats, err := s.WAL.Stats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if stats.AdmissionLimitBytes > 0 && stats.DiskBytes >= stats.AdmissionLimitBytes {
		http.Error(w, "WAL capacity exhausted", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if s.Collector != nil {
		m := s.Collector.Snapshot()
		fmt.Fprintln(w, "# TYPE modbus_collector_polls_total counter")
		fmt.Fprintf(w, "modbus_collector_polls_total %d\n", m.PollsTotal)
		fmt.Fprintln(w, "# TYPE modbus_collector_poll_errors_total counter")
		fmt.Fprintf(w, "modbus_collector_poll_errors_total %d\n", m.PollErrorsTotal)
		fmt.Fprintln(w, "# TYPE modbus_collector_poll_timeouts_total counter")
		fmt.Fprintf(w, "modbus_collector_poll_timeouts_total %d\n", m.PollTimeoutsTotal)
		fmt.Fprintln(w, "# TYPE modbus_collector_samples_total counter")
		fmt.Fprintf(w, "modbus_collector_samples_total %d\n", m.SamplesTotal)
		fmt.Fprintln(w, "# TYPE modbus_collector_inflight gauge")
		fmt.Fprintf(w, "modbus_collector_inflight %d\n", m.Inflight)
		fmt.Fprintln(w, "# TYPE modbus_collector_max_duration_seconds gauge")
		fmt.Fprintf(w, "modbus_collector_max_duration_seconds %f\n", m.MaxDuration.Seconds())
		if !m.LastPoll.IsZero() {
			fmt.Fprintln(w, "# TYPE modbus_collector_last_poll_timestamp_seconds gauge")
			fmt.Fprintf(w, "modbus_collector_last_poll_timestamp_seconds %d\n", m.LastPoll.Unix())
		}
	}
	if s.WAL != nil {
		stats, err := s.WAL.Stats()
		if err == nil {
			fmt.Fprintln(w, "# TYPE modbus_wal_disk_bytes gauge")
			fmt.Fprintf(w, "modbus_wal_disk_bytes %d\n", stats.DiskBytes)
			fmt.Fprintln(w, "# TYPE modbus_wal_disk_used_ratio gauge")
			fmt.Fprintf(w, "modbus_wal_disk_used_ratio %f\n", stats.DiskUsedRatio)
			fmt.Fprintln(w, "# TYPE modbus_wal_capacity_rejections_total counter")
			fmt.Fprintf(w, "modbus_wal_capacity_rejections_total %d\n", stats.CapacityRejections)
			fmt.Fprintln(w, "# TYPE modbus_wal_pending_records gauge")
			fmt.Fprintf(w, "modbus_wal_pending_records %d\n", stats.PendingRecords)
			fmt.Fprintln(w, "# TYPE modbus_wal_oldest_pending_age_seconds gauge")
			fmt.Fprintf(w, "modbus_wal_oldest_pending_age_seconds %f\n", stats.OldestPendingAge.Seconds())
			fmt.Fprintln(w, "# TYPE modbus_wal_segments gauge")
			fmt.Fprintf(w, "modbus_wal_segments %d\n", stats.SegmentCount)
			fmt.Fprintln(w, "# TYPE modbus_wal_checkpoint_sequence gauge")
			fmt.Fprintf(w, "modbus_wal_checkpoint_sequence %d\n", stats.CheckpointSeq)
		}
	}
	if s.Delivery != nil {
		m := s.Delivery.Snapshot()
		fmt.Fprintln(w, "# TYPE modbus_delivery_published_total counter")
		fmt.Fprintf(w, "modbus_delivery_published_total{sink=\"mqtt\"} %d\n", m.MQTT.Published)
		fmt.Fprintf(w, "modbus_delivery_published_total{sink=\"influx\"} %d\n", m.Influx.Published)
		fmt.Fprintln(w, "# TYPE modbus_delivery_publish_errors_total counter")
		fmt.Fprintf(w, "modbus_delivery_publish_errors_total{sink=\"mqtt\"} %d\n", m.MQTT.PublishErrors)
		fmt.Fprintf(w, "modbus_delivery_publish_errors_total{sink=\"influx\"} %d\n", m.Influx.PublishErrors)
		fmt.Fprintln(w, "# TYPE modbus_delivery_ack_errors_total counter")
		fmt.Fprintf(w, "modbus_delivery_ack_errors_total{sink=\"mqtt\"} %d\n", m.MQTT.AckErrors)
		fmt.Fprintf(w, "modbus_delivery_ack_errors_total{sink=\"influx\"} %d\n", m.Influx.AckErrors)
	}
}
