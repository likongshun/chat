package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTOptions struct {
	Broker   string
	ClientID string
	Username string
	Password string
	Topic    string
	Timeout  time.Duration
}

type MQTTSink struct {
	client  mqtt.Client
	topic   string
	timeout time.Duration
}

func NewMQTTSink(options MQTTOptions) (*MQTTSink, error) {
	if options.Broker == "" {
		return nil, fmt.Errorf("MQTT broker URL is required")
	}
	if options.ClientID == "" {
		options.ClientID = "modbus-collector"
	}
	if options.Topic == "" {
		options.Topic = "modbus/telemetry"
	}
	if options.Timeout <= 0 {
		options.Timeout = 10 * time.Second
	}
	clientOptions := mqtt.NewClientOptions().
		AddBroker(options.Broker).
		SetClientID(options.ClientID).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(time.Second).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectTimeout(options.Timeout).
		SetKeepAlive(30 * time.Second).
		SetOrderMatters(false)
	if options.Username != "" {
		clientOptions.SetUsername(options.Username)
		clientOptions.SetPassword(options.Password)
	}

	client := mqtt.NewClient(clientOptions)
	token := client.Connect()
	if !token.WaitTimeout(options.Timeout) {
		client.Disconnect(250)
		return nil, fmt.Errorf("MQTT connection timed out after %s", options.Timeout)
	}
	if err := token.Error(); err != nil {
		client.Disconnect(250)
		return nil, fmt.Errorf("connect MQTT broker: %w", err)
	}
	return &MQTTSink{client: client, topic: options.Topic, timeout: options.Timeout}, nil
}

func (s *MQTTSink) Name() string { return "mqtt" }

func (s *MQTTSink) Publish(ctx context.Context, events []Event) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("MQTT sink is not initialized")
	}
	if len(events) == 0 {
		return nil
	}
	if !s.client.IsConnected() {
		return fmt.Errorf("MQTT broker is disconnected")
	}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal MQTT event %d: %w", event.Seq, err)
		}
		token := s.client.Publish(s.topic, 1, false, payload)
		wait := s.timeout
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < wait {
				wait = remaining
			}
		}
		if wait <= 0 || !token.WaitTimeout(wait) {
			return fmt.Errorf("MQTT QoS 1 publish timed out for WAL sequence %d", event.Seq)
		}
		if err := token.Error(); err != nil {
			return fmt.Errorf("MQTT QoS 1 publish failed for WAL sequence %d: %w", event.Seq, err)
		}
	}
	return nil
}

func (s *MQTTSink) Close() {
	if s != nil && s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(250)
	}
}
