package observability

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/pipeline"
	"github.com/likongshun/chat/internal/reliability/wal"
)

func TestHealthReadinessAndPrometheusMetrics(t *testing.T) {
	store, err := wal.Open(wal.Options{Dir: t.TempDir(), MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append("metrics-event", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	server := &Server{
		Collector: &collector.Metrics{},
		WAL: store,
		Delivery: &pipeline.DeliveryMetrics{},
	}
	handler := server.Handler()

	for _, path := range []string{"/healthz", "/readyz"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	for _, metric := range []string{
		"modbus_collector_polls_total",
		"modbus_wal_pending_records 1",
		"modbus_wal_capacity_rejections_total",
		"modbus_delivery_published_total{sink=\"mqtt\"}",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics response missing %q: %s", metric, body)
		}
	}
}
