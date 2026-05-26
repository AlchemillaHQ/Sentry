package db

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/AlchemillaHQ/Sentry/auth"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type Database struct {
	Pool    *pgxpool.Pool
	Queries Querier
}

const schema = `
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(36) PRIMARY KEY,
    platform VARCHAR(10) NOT NULL,
    push_token BYTEA NOT NULL,
    upstream_host TEXT NOT NULL,
    upstream_port INTEGER NOT NULL DEFAULT 5060,
    upstream_transport VARCHAR(10) NOT NULL DEFAULT 'udp',
    upstream_user TEXT NOT NULL,
    upstream_password BYTEA NOT NULL,
    upstream_realm TEXT,
    display_name TEXT,
    b2bua_sip_user TEXT UNIQUE NOT NULL,
    device_contact TEXT,
    user_agent TEXT,
    push_provider VARCHAR(10),
    push_param TEXT,
    push_prid VARCHAR(512),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devices_b2bua_sip_user ON devices(b2bua_sip_user);
CREATE INDEX IF NOT EXISTS idx_devices_expires_at ON devices(expires_at);

CREATE TABLE IF NOT EXISTS pending_calls (
    call_id VARCHAR(36) PRIMARY KEY,
    device_id VARCHAR(36) NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    sip_call_id TEXT NOT NULL,
    sip_from TEXT NOT NULL,
    sip_to TEXT NOT NULL,
    sdp_offer TEXT,
    caller_uri TEXT NOT NULL,
    caller_name TEXT,
    state VARCHAR(30) NOT NULL DEFAULT 'PENDING_PUSH',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_calls_device_id ON pending_calls(device_id);
CREATE INDEX IF NOT EXISTS idx_pending_calls_expires_at ON pending_calls(expires_at);

CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

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
	// Drop all tables and recreate them from schema.
	tables := []string{"pending_calls", "devices", "settings", "users"}
	for _, t := range tables {
		_, err := db.Pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t))
		if err != nil {
			return fmt.Errorf("drop %s: %w", t, err)
		}
	}

	return db.Init(ctx)
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
