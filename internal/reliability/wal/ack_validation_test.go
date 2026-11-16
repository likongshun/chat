package wal

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAckRejectsUnknownSequenceAndMismatchedEvent(t *testing.T) {
	w, err := Open(Options{Dir: t.TempDir(), MaxBytes: 1 << 20, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()

	if err = w.Ack(Ack{Seq: 99, Sink: "mqtt", EventID: "missing"}); err == nil {
		t.Fatal("expected unknown sequence to be rejected")
	}
	r, err := w.Append("event-1", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	err = w.Ack(Ack{Seq: r.Seq, Sink: "mqtt", EventID: "wrong"})
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("expected event identity mismatch, got %v", err)
	}
	if err = w.Ack(Ack{Seq: r.Seq, Sink: "mqtt", EventID: "event-1"}); err != nil {
		t.Fatalf("valid ACK rejected: %v", err)
	}
}
