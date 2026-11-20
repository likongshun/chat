package telemetry

import "testing"

func TestRecordReconnectWithoutPollAttempt(t *testing.T) {
	stats := NewCommunicationStats()
	stats.RecordReconnect("meter-1")
	snapshot, ok := stats.Snapshot("meter-1")
	if !ok {
		t.Fatal("expected device counters after reconnect")
	}
	if snapshot.Reconnects != 1 || snapshot.Attempts != 0 {
		t.Fatalf("reconnect event polluted poll counters: %+v", snapshot)
	}
}
