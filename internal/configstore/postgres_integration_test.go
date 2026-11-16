package configstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/likongshun/chat/internal/config"
)

func TestPostgresConfigVersionImportAndActivate(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repository, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	firstID, err := repository.ImportXLSX(ctx, "../../configs/modbus_device_config.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	active, versionID, err := repository.LoadActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if versionID != firstID || len(active.Devices) == 0 || len(active.Points) == 0 {
		t.Fatalf("unexpected active config version=%d config=%+v", versionID, active)
	}

	cfg, err := config.LoadXLSX("../../configs/modbus_device_config.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := repository.Import(ctx, cfg, "integration-test")
	if err != nil {
		t.Fatal(err)
	}
	if secondID <= firstID {
		t.Fatalf("version ids are not increasing: first=%d second=%d", firstID, secondID)
	}
	if err := repository.Activate(ctx, firstID); err != nil {
		t.Fatal(err)
	}
	_, activeID, err := repository.LoadActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if activeID != firstID {
		t.Fatalf("active version=%d, want %d", activeID, firstID)
	}
	versions, err := repository.ListVersions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	foundActive := false
	for _, version := range versions {
		if version.ID == firstID && version.Active {
			foundActive = true
		}
	}
	if !foundActive {
		t.Fatal("version history did not mark restored version as active")
	}
}
