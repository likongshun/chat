package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestMQTTSinkQoS1Integration(t *testing.T) {
	broker := os.Getenv("MQTT_TEST_BROKER")
	if broker == "" {
		t.Skip("MQTT_TEST_BROKER is not configured")
	}
	topic := "modbus/integration/" + time.Now().Format("150405.000000000")
	received := make(chan []byte, 1)
	subscriberOptions := mqtt.NewClientOptions().AddBroker(broker).SetClientID("modbus-test-sub-" + time.Now().Format("150405000000000"))
	subscriber := mqtt.NewClient(subscriberOptions)
	if token := subscriber.Connect(); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("connect MQTT subscriber: %v", token.Error())
	}
	defer subscriber.Disconnect(250)
	sub := subscriber.Subscribe(topic, 1, func(_ mqtt.Client, message mqtt.Message) {
		select {
		case received <- append([]byte(nil), message.Payload()...):
		default:
		}
	})
	if !sub.WaitTimeout(5*time.Second) || sub.Error() != nil {
		t.Fatalf("subscribe MQTT topic: %v", sub.Error())
	}

	sink, err := NewMQTTSink(MQTTOptions{Broker: broker, ClientID: "modbus-test-pub-" + time.Now().Format("150405000000000"), Topic: topic, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	deadline := time.Now().Add(10 * time.Second)
	for !sink.client.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !sink.client.IsConnected() {
		t.Fatal("MQTT publisher did not connect")
	}
	event := Event{Seq: 11, ID: "integration-event", Payload: json.RawMessage(`{"value":42}`)}
	if err := sink.Publish(context.Background(), []Event{event}); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-received:
		var got Event
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatal(err)
		}
		if got.Seq != event.Seq || got.ID != event.ID {
			t.Fatalf("received event=%+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("MQTT subscriber did not receive QoS 1 event")
	}
}

func TestInfluxSinkIntegration(t *testing.T) {
	baseURL := os.Getenv("INFLUX_TEST_URL")
	if baseURL == "" {
		t.Skip("INFLUX_TEST_URL is not configured")
	}
	sink, err := NewInfluxSink(InfluxOptions{
		URL: baseURL,
		Org: os.Getenv("INFLUX_TEST_ORG"),
		Bucket: os.Getenv("INFLUX_TEST_BUCKET"),
		Token: os.Getenv("INFLUX_TEST_TOKEN"),
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		Seq: uint64(time.Now().UnixNano()),
		ID: "influx-integration",
		Payload: json.RawMessage(`{"device_id":"integration-device","model":"TEST","point_code":"value","unit":"unit","timestamp":"2026-01-02T03:04:05Z","value":42.5,"quality":"good"}`),
	}
	if err := sink.Publish(context.Background(), []Event{event}); err != nil {
		t.Fatal(err)
	}
}
