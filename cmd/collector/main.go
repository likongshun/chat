package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/configstore"
	"github.com/likongshun/chat/internal/pipeline"
	"github.com/likongshun/chat/internal/observability"
	"github.com/likongshun/chat/internal/reliability/wal"
	"github.com/likongshun/chat/internal/telemetry"
)

func main() {
	configPath := envString("CONFIG_XLSX", "./configs/modbus_device_config.xlsx")
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cfg, configVersion, configRepo, err := loadConfiguration(startupCtx, configPath)
	startupCancel()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}
	if configRepo != nil {
		defer configRepo.Close()
	}
	log.Printf("configuration loaded: version=%d devices=%d points=%d", configVersion, len(cfg.Devices), len(cfg.Points))
	configManager, err := config.NewManager(cfg)
	if err != nil {
		log.Fatalf("initialize configuration manager: %v", err)
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

	latestSamples := collector.NewLatestStore(envInt("LATEST_CACHE_MAX_POINTS", 1500000))
	communicationStats := telemetry.NewCommunicationStats()
	poller := &collector.ModbusPoller{
		Config: *configManager.Current(),
		Latest: latestSamples,
		ConfigVersion: configVersion,
		Communication: communicationStats,
		Timeout: envDuration("POLL_TIMEOUT", 800*time.Millisecond),
	}
	dlt645Poller := &collector.DL645Poller{Config: *configManager.Current(), Latest: latestSamples, ConfigVersion: configVersion, Timeout: envDuration("DLT645_TIMEOUT", 1500*time.Millisecond)}
	router := &collector.ProtocolRouter{Engines: map[string]collector.ProtocolPoller{"modbus-tcp": poller, "dl-t645-2007": dlt645Poller}}
	collectorMetrics := &collector.Metrics{}
	deliveryMetrics := &pipeline.DeliveryMetrics{}
	engine := &collector.Engine{
		Scheduler: collector.Scheduler{
			Workers: envInt("POLL_WORKERS", 500),
			OutputBuffer: envInt("SCHEDULER_OUTPUT_BUFFER", 10000),
			Metrics: collectorMetrics,
			DeviceStats: communicationStats,
		},
		Poller: poller,
		Router: router,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go monitorPointFreshness(ctx, configManager, latestSamples)

	observabilityDone := make(chan error, 1)
	observabilityServer := &observability.Server{
		Addr: envString("METRICS_ADDR", ":9090"),
		Config: configManager,
		Latest: latestSamples,
		DLT645: dlt645Poller,
		Influx: &observability.InfluxQueryOptions{URL: envString("INFLUXDB_URL", "http://influxdb:8086"), Org: envString("INFLUXDB_ORG", "modbus"), Bucket: envString("INFLUXDB_BUCKET", "telemetry"), Token: os.Getenv("INFLUXDB_TOKEN")},
		Collector: collectorMetrics,
		Communication: communicationStats,
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
	deliveryCtx, cancelDelivery := context.WithCancel(context.Background())
	defer cancelDelivery()
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
		go func() { deliveryDone <- worker.Run(deliveryCtx) }()
	} else {
		log.Printf("downstream delivery is not configured; WAL will buffer samples")
	}
	updates := make(chan configUpdate, 1)
	if configRepo != nil {
		go watchActiveConfiguration(ctx, configRepo, configVersion, envDuration("CONFIG_RELOAD_INTERVAL", 5*time.Second), updates)
	}
	if err := runCollection(ctx, engine, poller, dlt645Poller, configManager, cfg, configVersion, store, updates); err != nil {
		log.Printf("collection stopped: %v", err)
	}
	cancelDelivery()
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

type configUpdate struct {
	Config config.Config
	Version int64
}

func runCollection(
	ctx context.Context,
	engine *collector.Engine,
	poller *collector.ModbusPoller,
	dlt645Poller *collector.DL645Poller,
	manager *config.Manager,
	cfg config.Config,
	version int64,
	store *wal.WAL,
	updates <-chan configUpdate,
) error {
	for {
		if !hasEnabledDevices(cfg.Devices) {
			select {
			case <-ctx.Done():
				return nil
			case update := <-updates:
				if err := manager.Reload(update.Config); err != nil {
					log.Printf("reject configuration version %d: %v", update.Version, err)
					continue
				}
				cfg = *manager.Current()
				poller.Config = cfg
				poller.ConfigVersion = update.Version
				dlt645Poller.Config = cfg
				dlt645Poller.ConfigVersion = update.Version
				version = update.Version
				log.Printf("activated configuration version %d", version)
				continue
			}
		}

		pollCtx, cancelPoll := context.WithCancel(ctx)
		results := engine.Run(pollCtx, collectionDevices(cfg.Devices))
		writerCtx, cancelWriter := context.WithCancel(context.Background())
		persistDone := make(chan error, 1)
		go func() { persistDone <- pipeline.PersistResults(writerCtx, results, store) }()

		select {
		case <-ctx.Done():
			cancelPoll()
			drainTimer := time.NewTimer(envDuration("PERSIST_DRAIN_TIMEOUT", 30*time.Second))
			var persistErr error
			select {
			case persistErr = <-persistDone:
			case <-drainTimer.C:
				cancelWriter()
				persistErr = <-persistDone
				for range results {
					// Discard only after the bounded shutdown drain expired; wait
					// for scheduler workers to exit before closing Modbus clients.
				}
				log.Printf("WAL writer drain timed out during shutdown")
			}
			if !drainTimer.Stop() {
				select { case <-drainTimer.C: default: }
			}
			cancelWriter()
			_ = engine.Close()
			if persistErr != nil && !errors.Is(persistErr, context.Canceled) {
				log.Printf("final WAL drain: %v", persistErr)
			}
			return nil

		case update := <-updates:
			cancelPoll()
			persistErr := <-persistDone
			cancelWriter()
			if persistErr != nil {
				for range results {
				}
				_ = engine.Close()
				return fmt.Errorf("flush samples before configuration switch: %w", persistErr)
			}
			if err := engine.Close(); err != nil {
				log.Printf("close clients before config switch: %v", err)
			}
			if err := manager.Reload(update.Config); err != nil {
				log.Printf("reject configuration version %d: %v", update.Version, err)
				continue
			}
			cfg = *manager.Current()
			poller.Config = cfg
			poller.ConfigVersion = update.Version
			dlt645Poller.Config = cfg
			dlt645Poller.ConfigVersion = update.Version
			version = update.Version
			log.Printf("activated configuration version %d; devices=%d points=%d", version, len(cfg.Devices), len(cfg.Points))

		case persistErr := <-persistDone:
			cancelPoll()
			cancelWriter()
			if persistErr != nil {
				for range results {
				}
			}
			_ = engine.Close()
			if persistErr != nil {
				return persistErr
			}
			// A scheduler with no enabled devices closes immediately; wait for
			// either a new DB version or process shutdown instead of spinning.
			if !hasEnabledDevices(cfg.Devices) {
				continue
			}
			return fmt.Errorf("collection result stream closed unexpectedly")
		}
	}
}

func collectionDevices(devices []config.Device) []config.Device {
	active := make([]config.Device, 0, len(devices))
	for _, device := range devices {
		if device.Enabled && !device.DiscoveryOnly { active = append(active, device) }
	}
	return active
}

func hasEnabledDevices(devices []config.Device) bool {
	for _, device := range devices {
		if device.Enabled && !device.DiscoveryOnly {
			return true
		}
	}
	return false
}

func watchActiveConfiguration(ctx context.Context, repository *configstore.Repository, currentVersion int64, interval time.Duration, updates chan<- configUpdate) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg, version, err := repository.LoadActive(ctx)
			if err != nil {
				log.Printf("configuration version poll failed: %v", err)
				continue
			}
			if version == currentVersion {
				continue
			}
			select {
			case updates <- configUpdate{Config: cfg, Version: version}:
				currentVersion = version
			case <-ctx.Done():
				return
			}
		}
	}
}

func loadConfiguration(ctx context.Context, path string) (config.Config, int64, *configstore.Repository, error) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = postgresDSN()
	}
	if dsn == "" {
		cfg, err := config.LoadXLSX(path)
		return cfg, 0, nil, err
	}
	repository, err := configstore.Open(ctx, dsn)
	if err != nil {
		return config.Config{}, 0, nil, err
	}
	fail := func(err error) (config.Config, int64, *configstore.Repository, error) {
		repository.Close()
		return config.Config{}, 0, nil, err
	}
	if err := repository.Migrate(ctx); err != nil {
		return fail(err)
	}
	if envBool("CONFIG_IMPORT_ON_START", false) {
		if _, err := repository.ImportXLSX(ctx, path); err != nil {
			return fail(err)
		}
	}
	cfg, version, err := repository.LoadActive(ctx)
	if errors.Is(err, configstore.ErrNoActiveVersion) {
		if _, importErr := repository.ImportXLSX(ctx, path); importErr != nil {
			return fail(importErr)
		}
		cfg, version, err = repository.LoadActive(ctx)
	}
	if err != nil {
		return fail(err)
	}
	return cfg, version, repository, nil
}

func postgresDSN() string {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		return ""
	}
	port := envString("POSTGRES_PORT", "5432")
	user := envString("POSTGRES_USER", "modbus")
	password := os.Getenv("POSTGRES_PASSWORD")
	database := envString("POSTGRES_DB", "modbus")
	u := &url.URL{
		Scheme: "postgres",
		User: url.UserPassword(user, password),
		Host: net.JoinHostPort(host, port),
		Path: database,
	}
	query := u.Query()
	query.Set("sslmode", envString("POSTGRES_SSLMODE", "disable"))
	u.RawQuery = query.Encode()
	return u.String()
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
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

func monitorPointFreshness(ctx context.Context, manager *config.Manager, latest *collector.LatestStore) {
 ticker:=time.NewTicker(5*time.Second)
 defer ticker.Stop()
 var cached *config.Config
 var expected map[string]time.Duration
 evaluate:=func() {
  cfg:=manager.ReadOnly()
  if cfg==nil { return }
  if cfg!=cached {
   next:=make(map[string]time.Duration)
   for _,device:=range cfg.Devices {
    if !device.Enabled || device.DiscoveryOnly || device.Interval<=0 { continue }
    for _,point:=range cfg.Points {
     if !point.Enabled { continue }
     if point.Model==device.Model || point.DeviceID==device.ID {
      next[collector.PointIntervalKey(device.ID,point.PointCode)]=device.Interval
     }
    }
   }
   cached=cfg
   expected=next
  }
  latest.MarkStale(time.Now().UTC(),expected,2)
 }
 evaluate()
 for {
  select {
  case <-ctx.Done(): return
  case <-ticker.C: evaluate()
  }
 }
}
