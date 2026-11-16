package pipeline

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInfluxSinkWritesLineProtocolBatch(t *testing.T) {
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/write" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if r.URL.Query().Get("org") != "modbus-org" || r.URL.Query().Get("bucket") != "telemetry" || r.URL.Query().Get("precision") != "ns" {
			t.Errorf("query=%v", r.URL.Query())
		}
		if r.Header.Get("Authorization") != "Token test-token" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := NewInfluxSink(InfluxOptions{URL: server.URL, Org: "modbus-org", Bucket: "telemetry", Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		Seq: 7,
		ID: "event-7",
		Payload: []byte(`{"device_id":"d1","model":"METER_A","point_code":"voltage","timestamp":"2026-01-02T03:04:05Z","value":220.5,"quality":"good"}`),
	}
	if err := sink.Publish(context.Background(), []Event{event}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotBody, "modbus_point,device_id=d1,model=METER_A,point_code=voltage,quality=good") {
		t.Fatalf("unexpected line protocol: %s", gotBody)
	}
	if !strings.Contains(gotBody, "value=220.5,wal_seq=7i") {
		t.Fatalf("missing numeric fields: %s", gotBody)
	}
}

func TestInfluxSinkReportsHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "write rejected", http.StatusBadRequest)
	}))
	defer server.Close()
	sink, err := NewInfluxSink(InfluxOptions{URL: server.URL, Org: "o", Bucket: "b", Token: "t", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{Seq: 1, Payload: []byte(`{"device_id":"d","point_code":"p","timestamp":"2026-01-02T03:04:05Z","value":1,"quality":"good"}`)}
	if err := sink.Publish(context.Background(), []Event{event}); err == nil || !strings.Contains(err.Error(), "write rejected") {
		t.Fatalf("expected InfluxDB rejection, got %v", err)
	}
}
