package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/pipeline"
	"github.com/likongshun/chat/internal/observability"
	"github.com/likongshun/chat/internal/reliability/wal"
)

func main() {
	configPath := envString("CONFIG_XLSX", "./configs/modbus_device_config.xlsx")
	cfg, err := config.LoadXLSX(configPath)
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	dir := envString("WAL_DIR", "./data/wal")
	store, err := wal.Open(wal.Options{
		Dir: dir,
		SegmentBytes: envInt64("WAL_SEGMENT_BYTES", 64<<20),
		MaxBytes: envInt64("MAX_WAL_BYTES", 10<<30),
		ReserveBytes: envInt64("ACK_RESERVE_BYTES", 64<<20),
		Sync: true,
	})
	if err != nil {
		log.Fatalf("open WAL: %v", err)
	}

	poller := &collector.ModbusPoller{
		Config: cfg,
		Timeout: envDuration("POLL_TIMEOUT", 800*time.Millisecond),
	}
	collectorMetrics := &collector.Metrics{}
	deliveryMetrics := &pipeline.DeliveryMetrics{}
	engine := &collector.Engine{
		Scheduler: collector.Scheduler{
			Workers: envInt("POLL_WORKERS", 500),
			OutputBuffer: envInt("SCHEDULER_OUTPUT_BUFFER", 10000),
			Metrics: collectorMetrics,
		},
		Poller: poller,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	observabilityDone := make(chan error, 1)
	observabilityServer := &observability.Server{
		Addr: envString("METRICS_ADDR", ":9090"),
		Collector: collectorMetrics,
		WAL: store,
		Delivery: deliveryMetrics,
	}
	go func() { observabilityDone <- observabilityServer.Run(ctx) }()

	mqttSink, err := pipeline.NewMQTTSink(pipeline.MQTTOptions{
		Broker: envString("MQTT_BROKER", "tcp://mqtt:1883"),
		ClientID: envString("MQTT_CLIENT_ID", "modbus-collector"),
		Username: os.Getenv("MQTT_USERNAME"),
		Password: os.Getenv("MQTT_PASSWORD"),
		Topic: envString("MQTT_TOPIC", "modbus/telemetry"),
		Timeout: envDuration("SINK_TIMEOUT", 10*time.Second),
	})
	if err != nil {
		log.Fatalf("configure MQTT sink: %v", err)
	}
	influxSink, err := pipeline.NewInfluxSink(pipeline.InfluxOptions{
		URL: envString("INFLUXDB_URL", "http://influxdb:8086"),
		Org: envString("INFLUXDB_ORG", "modbus"),
		Bucket: envString("INFLUXDB_BUCKET", "telemetry"),
		Token: os.Getenv("INFLUXDB_TOKEN"),
		Timeout: envDuration("SINK_TIMEOUT", 10*time.Second),
	})
	if err != nil {
		log.Printf("InfluxDB sink disabled: %v; records will remain in WAL", err)
	}
	var deliveryDone chan error
	if influxSink != nil {
		deliveryDone = make(chan error, 1)
		worker := &pipeline.DeliveryWorker{
			Store: store,
			Sinks: []pipeline.Sink{mqttSink, influxSink},
			BatchSize: envInt("DELIVERY_BATCH_SIZE", 500),
			PollInterval: envDuration("DELIVERY_POLL_INTERVAL", time.Second),
			RetryInterval: envDuration("DELIVERY_RETRY_INTERVAL", time.Second),
			Metrics: deliveryMetrics,
			OnError: func(err error) { log.Printf("delivery retry: %v", err) },
		}
		go func() { deliveryDone <- worker.Run(ctx) }()
	} else {
		log.Printf("downstream delivery is not configured; WAL will buffer samples")
	}
	results := engine.Run(ctx, cfg.Devices)
	log.Printf("collector started: devices=%d points=%d WAL=%s workers=%d",
		len(cfg.Devices), len(cfg.Points), dir, envInt("POLL_WORKERS", 500))

	if err := pipeline.PersistResults(results, store); err != nil {
		log.Printf("persist collection result: %v; stopping collection", err)
		stop()
	}
	stop()
	if err := <-observabilityDone; err != nil {
		log.Printf("observability server: %v", err)
	}
	if deliveryDone != nil {
		if err := <-deliveryDone; err != nil {
			log.Printf("delivery worker stopped: %v", err)
		}
	}
	mqttSink.Close()
	if err := engine.Close(); err != nil {
		log.Printf("close Modbus clients: %v", err)
	}
	if err := store.Close(); err != nil {
		log.Printf("close WAL: %v", err)
	}
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envInt64(key string, fallback int64) int64 {
	value, err := strconv.ParseInt(os.Getenv(key), 10, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
