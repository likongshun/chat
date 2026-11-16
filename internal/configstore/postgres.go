package configstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/likongshun/chat/internal/config"
)

var ErrNoActiveVersion = errors.New("no active configuration version")

type Repository struct {
	pool *pgxpool.Pool
}

type Version struct {
	ID        int64
	Checksum  string
	Source    string
	CreatedAt time.Time
	Active    bool
}

func Open(ctx context.Context, dsn string) (*Repository, error) {
	if dsn == "" {
		return nil, fmt.Errorf("PostgreSQL DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *Repository) Migrate(ctx context.Context) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("configuration repository is not initialized")
	}
	schema := "CREATE TABLE IF NOT EXISTS modbus_config_versions (" +
		"id BIGSERIAL PRIMARY KEY, checksum TEXT NOT NULL, source TEXT NOT NULL, " +
		"snapshot JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());" +
		"CREATE TABLE IF NOT EXISTS modbus_config_active (" +
		"singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), " +
		"version_id BIGINT NOT NULL REFERENCES modbus_config_versions(id), " +
		"updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());" +
		"CREATE INDEX IF NOT EXISTS idx_modbus_config_versions_created_at " +
		"ON modbus_config_versions (created_at DESC);"
	if _, err := r.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("migrate configuration schema: %w", err)
	}
	return nil
}

// ImportXLSX validates the workbook before storing an immutable version and
// switching the active pointer in one PostgreSQL transaction.
func (r *Repository) ImportXLSX(ctx context.Context, path string) (int64, error) {
	cfg, err := config.LoadXLSX(path)
	if err != nil {
		return 0, fmt.Errorf("load Excel configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return 0, fmt.Errorf("validate Excel configuration: %w", err)
	}
	return r.Import(ctx, cfg, path)
}

func (r *Repository) Import(ctx context.Context, cfg config.Config, source string) (int64, error) {
	if err := cfg.Validate(); err != nil {
		return 0, fmt.Errorf("validate configuration: %w", err)
	}
	snapshot, err := json.Marshal(cfg)
	if err != nil {
		return 0, fmt.Errorf("marshal configuration snapshot: %w", err)
	}
	sum := sha256.Sum256(snapshot)
	checksum := hex.EncodeToString(sum[:])
	if source == "" {
		source = "api"
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var versionID int64
	if err := tx.QueryRow(ctx,
		"INSERT INTO modbus_config_versions (checksum, source, snapshot) VALUES ($1, $2, $3) RETURNING id",
		checksum, source, snapshot,
	).Scan(&versionID); err != nil {
		return 0, fmt.Errorf("insert configuration version: %w", err)
	}
	activateSQL := "INSERT INTO modbus_config_active (singleton, version_id) VALUES (TRUE, $1) " +
		"ON CONFLICT (singleton) DO UPDATE SET version_id = EXCLUDED.version_id, updated_at = NOW()"
	if _, err := tx.Exec(ctx, activateSQL, versionID); err != nil {
		return 0, fmt.Errorf("activate imported configuration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit configuration import: %w", err)
	}
	return versionID, nil
}

func (r *Repository) LoadActive(ctx context.Context) (config.Config, int64, error) {
	var versionID int64
	var checksum string
	var snapshot []byte
	query := "SELECT v.id, v.checksum, v.snapshot FROM modbus_config_active a " +
		"JOIN modbus_config_versions v ON v.id = a.version_id WHERE a.singleton = TRUE"
	err := r.pool.QueryRow(ctx, query).Scan(&versionID, &checksum, &snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return config.Config{}, 0, ErrNoActiveVersion
	}
	if err != nil {
		return config.Config{}, 0, fmt.Errorf("load active configuration: %w", err)
	}
	sum := sha256.Sum256(snapshot)
	if hex.EncodeToString(sum[:]) != checksum {
		return config.Config{}, 0, fmt.Errorf("configuration version %d checksum mismatch", versionID)
	}
	var cfg config.Config
	if err := json.Unmarshal(snapshot, &cfg); err != nil {
		return config.Config{}, 0, fmt.Errorf("decode configuration version %d: %w", versionID, err)
	}
	if err := cfg.Validate(); err != nil {
		return config.Config{}, 0, fmt.Errorf("stored configuration version %d is invalid: %w", versionID, err)
	}
	return cfg, versionID, nil
}

func (r *Repository) Activate(ctx context.Context, versionID int64) error {
	var checksum string
	var snapshot []byte
	if err := r.pool.QueryRow(ctx,
		"SELECT checksum, snapshot FROM modbus_config_versions WHERE id = $1", versionID,
	).Scan(&checksum, &snapshot); err != nil {
		return fmt.Errorf("load configuration version %d: %w", versionID, err)
	}
	sum := sha256.Sum256(snapshot)
	if hex.EncodeToString(sum[:]) != checksum {
		return fmt.Errorf("configuration version %d checksum mismatch", versionID)
	}
	var cfg config.Config
	if err := json.Unmarshal(snapshot, &cfg); err != nil {
		return fmt.Errorf("decode configuration version %d: %w", versionID, err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("configuration version %d is invalid: %w", versionID, err)
	}
	activateSQL := "INSERT INTO modbus_config_active (singleton, version_id) VALUES (TRUE, $1) " +
		"ON CONFLICT (singleton) DO UPDATE SET version_id = EXCLUDED.version_id, updated_at = NOW()"
	if _, err := r.pool.Exec(ctx, activateSQL, versionID); err != nil {
		return fmt.Errorf("activate configuration version %d: %w", versionID, err)
	}
	return nil
}

func (r *Repository) ListVersions(ctx context.Context, limit int) ([]Version, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := "SELECT v.id, v.checksum, v.source, v.created_at, (a.version_id = v.id) AS active " +
		"FROM modbus_config_versions v LEFT JOIN modbus_config_active a ON a.singleton = TRUE " +
		"ORDER BY v.id DESC LIMIT $1"
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := make([]Version, 0)
	for rows.Next() {
		var item Version
		if err := rows.Scan(&item.ID, &item.Checksum, &item.Source, &item.CreatedAt, &item.Active); err != nil {
			return nil, err
		}
		versions = append(versions, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return versions, nil
}
