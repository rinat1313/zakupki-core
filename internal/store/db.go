package store

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://zakupki:zakupki@localhost:5432/zakupki?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 24
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour

	var pool *pgxpool.Pool
	var lastErr error
	for i := 0; i < 30; i++ {
		pool, lastErr = pgxpool.NewWithConfig(ctx, cfg)
		if lastErr == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				_ = Migrate(ctx, pool)
				return pool, nil
			} else {
				lastErr = pingErr
				pool.Close()
			}
		}
		time.Sleep(time.Second)
	}
	return nil, fmt.Errorf("db connect: %w", lastErr)
}

// Migrate applies idempotent schema upgrades for existing volumes.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		`ALTER TYPE ingest_item_status ADD VALUE IF NOT EXISTS 'failed_analyze'`,
		`ALTER TYPE ingest_item_status ADD VALUE IF NOT EXISTS 'cancelled'`,
		`ALTER TYPE analysis_status ADD VALUE IF NOT EXISTS 'analyzing'`,
		`CREATE TABLE IF NOT EXISTS ingest_job_logs (
		  id           BIGSERIAL PRIMARY KEY,
		  job_id       UUID NOT NULL REFERENCES ingest_jobs(id) ON DELETE CASCADE,
		  item_id      UUID REFERENCES ingest_job_items(id) ON DELETE SET NULL,
		  reg_number   TEXT NOT NULL DEFAULT '',
		  level        TEXT NOT NULL DEFAULT 'info',
		  message      TEXT NOT NULL DEFAULT '',
		  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS ingest_job_logs_job_idx ON ingest_job_logs(job_id, id)`,
		`CREATE TABLE IF NOT EXISTS ai_configs (
		  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		  category_id    UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
		  name           TEXT NOT NULL,
		  system_prompt  TEXT NOT NULL DEFAULT '',
		  user_prompt    TEXT NOT NULL DEFAULT '',
		  rules          TEXT NOT NULL DEFAULT '',
		  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
		  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
		  UNIQUE (category_id, name)
		)`,
		`ALTER TABLE categories ADD COLUMN IF NOT EXISTS active_ai_config_id UUID`,
		`ALTER TABLE categories ADD COLUMN IF NOT EXISTS search_config_id TEXT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS categories_search_config_id_uidx
		   ON categories (search_config_id)
		   WHERE search_config_id IS NOT NULL AND search_config_id <> ''`,
		`ALTER TABLE tenders ADD COLUMN IF NOT EXISTS collect_pct INT NOT NULL DEFAULT 0`,
		`ALTER TABLE tenders ADD COLUMN IF NOT EXISTS ai_pct INT NOT NULL DEFAULT 0`,
		`ALTER TABLE tenders ADD COLUMN IF NOT EXISTS retained BOOLEAN NOT NULL DEFAULT FALSE`,
		`ALTER TABLE tenders ADD COLUMN IF NOT EXISTS retained_at TIMESTAMPTZ`,
		`ALTER TABLE tenders ADD COLUMN IF NOT EXISTS retain_reason TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS tenders_retained_idx ON tenders (retained) WHERE retained`,
	}
	for _, q := range stmts {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
