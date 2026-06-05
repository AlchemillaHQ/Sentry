package db

import (
	"context"
	"crypto/rand"
	_ "embed"
	"fmt"
	"time"

	"github.com/AlchemillaHQ/Sentry/auth"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

//go:embed schema.sql
var schema string

const resetPrefix = `
DROP TABLE IF EXISTS settings CASCADE;
DROP TABLE IF EXISTS pending_calls CASCADE;
DROP TABLE IF EXISTS devices CASCADE;
DROP TABLE IF EXISTS users CASCADE;
`

type Database struct {
	Pool    *pgxpool.Pool
	Queries Querier
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

	db := &Database{
		Pool:    pool,
		Queries: New(pool),
	}

	if err := db.Init(ctx); err != nil {
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return db, nil
}

func (db *Database) Init(ctx context.Context) error {
	_, err := db.Pool.Exec(ctx, schema)
	return err
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

func (db *Database) Reset(ctx context.Context) error {
	log.Info().Msg("performing full database reset (DROP + CREATE)...")
	_, err := db.Pool.Exec(ctx, resetPrefix+schema)
	if err != nil {
		return fmt.Errorf("reset schema: %w", err)
	}
	return nil
}

func (db *Database) CleanupStaleState(ctx context.Context) {
	now := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	err := db.Queries.PruneDevices(ctx, now)
	if err != nil {
		log.Error().Err(err).Msg("failed to prune devices")
	}

	err = db.Queries.PrunePendingCalls(ctx, now)
	if err != nil {
		log.Error().Err(err).Msg("failed to prune pending calls")
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

func (db *Database) BootstrapUsers(ctx context.Context, bootstrapUsers []config.BootstrapUser) error {
	for _, u := range bootstrapUsers {
		hash, err := auth.HashPassword(u.Password)
		if err != nil {
			return fmt.Errorf("hash password for %s: %w", u.Username, err)
		}

		err = db.Queries.CreateUser(ctx, CreateUserParams{
			Username:     u.Username,
			PasswordHash: hash,
			Role:         "admin",
		})
		if err != nil {
			return fmt.Errorf("bootstrap user %s: %w", u.Username, err)
		}
		log.Info().Str("username", u.Username).Msg("bootstrapped admin user")
	}
	return nil
}
