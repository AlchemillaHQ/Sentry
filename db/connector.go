package db

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	Pool    *pgxpool.Pool
	Queries *Queries
}

func Open(ctx context.Context, cfg config.DatabaseConfig) (*Database, error) {
	if cfg.Driver != "postgres" {
		return nil, fmt.Errorf("only postgres driver is supported with SQLC migration")
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Database{
		Pool:    pool,
		Queries: New(pool),
	}, nil
}

func (db *Database) GetOrCreateEncryptionKey(ctx context.Context) ([]byte, error) {
	val, err := db.Queries.GetSetting(ctx, "encryption_key")
	if err == nil {
		return val, nil
	}
	if err != pgx.ErrNoRows {
		return nil, fmt.Errorf("query setting: %w", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	err = db.Queries.UpsertSetting(ctx, UpsertSettingParams{
		Key:   "encryption_key",
		Value: key,
	})
	if err != nil {
		return nil, fmt.Errorf("store key: %w", err)
	}

	return key, nil
}

func (db *Database) CleanupStaleState(ctx context.Context) {
	now := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	err := db.Queries.PruneDevices(ctx, now)
	if err != nil {
		slog.Error("failed to prune devices", "error", err)
	}

	err = db.Queries.PrunePendingCalls(ctx, now)
	if err != nil {
		slog.Error("failed to prune pending calls", "error", err)
	}
}

func (db *Database) StartCleanupWorker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				db.CleanupStaleState(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

