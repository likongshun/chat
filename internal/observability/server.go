package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/likongshun/chat/internal/config"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/pipeline"
	"github.com/likongshun/chat/internal/reliability/wal"
	"github.com/likongshun/chat/internal/telemetry"
)

type Server struct {
	Addr      string
	Config    *config.Manager
	Latest    *collector.LatestStore
	DLT645    *collector.DL645Poller
	Influx    *InfluxQueryOptions
	InfluxClient *http.Client
	Collector *collector.Metrics
	Communication *telemetry.CommunicationStats
	WAL       *wal.WAL
	Delivery  *pipeline.DeliveryMetrics

	configIndexMu sync.RWMutex
	indexedConfig *config.Config
	pointsByModel map[string][]config.Point
	pointCounts map[string]int
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
 s.registerDataAPI(mux)
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
	if stats.DiskLevel == "stop" || (stats.AdmissionLimitBytes > 0 && stats.DiskBytes >= stats.AdmissionLimitBytes) {
		http.Error(w, "WAL or filesystem capacity exhausted", http.StatusServiceUnavailable)
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
	if s.Communication != nil {
		m := s.Communication.FleetSnapshot()
		fmt.Fprintln(w, "# TYPE dcim_edge_devices_tracked gauge")
		fmt.Fprintf(w, "dcim_edge_devices_tracked %d\\n", m.Devices)
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_attempts_total counter")
		fmt.Fprintf(w, "dcim_edge_communication_attempts_total %d\\n", m.Attempts)
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_successes_total counter")
		fmt.Fprintf(w, "dcim_edge_communication_successes_total %d\\n", m.Successes)
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_failures_total counter")
		fmt.Fprintf(w, "dcim_edge_communication_failures_total %d\\n", m.Failures)
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_timeouts_total counter")
		fmt.Fprintf(w, "dcim_edge_communication_timeouts_total %d\\n", m.Timeouts)
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_reconnects_total counter")
		fmt.Fprintf(w, "dcim_edge_communication_reconnects_total %d\\n", m.Reconnects)
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_latency_seconds_sum summary")
		fmt.Fprintf(w, "dcim_edge_communication_latency_seconds_sum %f\\n", m.LatencyTotal.Seconds())
		fmt.Fprintln(w, "# TYPE dcim_edge_communication_latency_seconds_count summary")
		fmt.Fprintf(w, "dcim_edge_communication_latency_seconds_count %d\\n", m.LatencyCount)
		for _, protocol := range s.Communication.ProtocolSnapshots() {
			fmt.Fprintf(w, "dcim_edge_protocol_poll_attempts_total{protocol=%q} %d\\n", protocol.Protocol, protocol.Attempts)
			fmt.Fprintf(w, "dcim_edge_protocol_poll_successes_total{protocol=%q} %d\\n", protocol.Protocol, protocol.Successes)
			fmt.Fprintf(w, "dcim_edge_protocol_poll_failures_total{protocol=%q} %d\\n", protocol.Protocol, protocol.Failures)
			fmt.Fprintf(w, "dcim_edge_protocol_poll_timeouts_total{protocol=%q} %d\\n", protocol.Protocol, protocol.Timeouts)
		}
	}
	if s.Latest != nil {
		counts := s.Latest.QualityCounts()
		fmt.Fprintln(w, "# TYPE dcim_edge_points_by_quality gauge")
		for _, quality := range []collector.Quality{collector.QualityGood, collector.QualityBad, collector.QualityCommError, collector.QualityUncertain, collector.QualityStale} {
			fmt.Fprintf(w, "dcim_edge_points_by_quality{quality=%q} %d\\n", quality, counts[quality])
		}
	}
	if s.WAL != nil {
		stats, err := s.WAL.Stats()
		if err == nil {
			fmt.Fprintln(w, "# TYPE modbus_wal_disk_bytes gauge")
			fmt.Fprintf(w, "modbus_wal_disk_bytes %d\n", stats.DiskBytes)
			fmt.Fprintln(w, "# TYPE modbus_wal_disk_used_ratio gauge")
			fmt.Fprintf(w, "modbus_wal_disk_used_ratio %f\n", stats.DiskUsedRatio)
			fmt.Fprintln(w, "# TYPE modbus_wal_filesystem_used_ratio gauge")
			fmt.Fprintf(w, "modbus_wal_filesystem_used_ratio %f\n", stats.FilesystemUsedRatio)
			fmt.Fprintln(w, "# TYPE modbus_wal_disk_level gauge")
			diskLevel := map[string]float64{"normal": 0, "warning": 1, "critical": 2, "stop": 3}[stats.DiskLevel]
			fmt.Fprintf(w, "modbus_wal_disk_level %g\n", diskLevel)
			fmt.Fprintln(w, "# TYPE modbus_wal_capacity_rejections_total counter")
			fmt.Fprintf(w, "modbus_wal_capacity_rejections_total %d\n", stats.CapacityRejections)
			fmt.Fprintln(w, "# TYPE modbus_wal_pending_records gauge")
			fmt.Fprintf(w, "modbus_wal_pending_records %d\n", stats.PendingRecords)
			fmt.Fprintln(w, "# TYPE modbus_wal_oldest_pending_age_seconds gauge")
			fmt.Fprintf(w, "modbus_wal_oldest_pending_age_seconds %f\n", stats.OldestPendingAge.Seconds())
			fmt.Fprintln(w, "# TYPE modbus_wal_segments gauge")
			fmt.Fprintf(w, "modbus_wal_segments %d\n", stats.SegmentCount)
			fmt.Fprintln(w, "# TYPE modbus_wal_ack_segments gauge")
			fmt.Fprintf(w, "modbus_wal_ack_segments %d\n", stats.AckSegmentCount)
			fmt.Fprintln(w, "# TYPE modbus_wal_checkpoint_sequence gauge")
			fmt.Fprintf(w, "modbus_wal_checkpoint_sequence %d\n", stats.CheckpointSeq)
			fmt.Fprintln(w, "# TYPE modbus_wal_fsync_duration_seconds histogram")
			bounds := []string{"0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "1"}
			var cumulative uint64
			for i, bound := range bounds {
				cumulative += stats.Fsync.Buckets[i]
				fmt.Fprintf(w, "modbus_wal_fsync_duration_seconds_bucket{le=%q} %d\n", bound, cumulative)
			}
			cumulative += stats.Fsync.Buckets[len(stats.Fsync.Buckets)-1]
			fmt.Fprintf(w, "modbus_wal_fsync_duration_seconds_bucket{le=%q} %d\n", "+Inf", cumulative)
			fmt.Fprintf(w, "modbus_wal_fsync_duration_seconds_sum %f\n", stats.Fsync.Total.Seconds())
			fmt.Fprintf(w, "modbus_wal_fsync_duration_seconds_count %d\n", stats.Fsync.Count)
			fmt.Fprintln(w, "# TYPE modbus_wal_fsync_errors_total counter")
			fmt.Fprintf(w, "modbus_wal_fsync_errors_total %d\n", stats.Fsync.Errors)
			fmt.Fprintln(w, "# TYPE modbus_wal_fsync_p99_seconds gauge")
			fmt.Fprintf(w, "modbus_wal_fsync_p99_seconds %f\n", stats.Fsync.P99.Seconds())
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
