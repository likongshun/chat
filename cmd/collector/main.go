package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/reliability/wal"
)

func main() {
	configPath := os.Getenv("CONFIG_XLSX")
	if configPath == "" {
		configPath = "./configs/modbus_device_config.xlsx"
	}
	cfg, err := config.LoadXLSX(configPath)
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	dir := os.Getenv("WAL_DIR")
	if dir == "" {
		dir = "./data/wal"
	}
	w, err := wal.Open(wal.Options{
		Dir: dir, SegmentBytes: 64 << 20, MaxBytes: 10 << 30,
		ReserveBytes: 64 << 20, Sync: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("collector core initialized: devices=%d points=%d WAL=%s",
		len(cfg.Devices), len(cfg.Points), dir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	if err := w.Close(); err != nil {
		log.Printf("close WAL: %v", err)
	}
}
