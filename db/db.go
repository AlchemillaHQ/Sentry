package db

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/AlchemillaHQ/Difuse-B2BUA/config"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func Open(cfg config.DatabaseConfig) (*gorm.DB, error) {
	var dialector gorm.Dialector

	switch cfg.Driver {
	case "sqlite":
		dsn := cfg.DSN
		if cfg.Driver == "sqlite" && dsn != "" {
			if dsn[len(dsn)-1] != '?' {
				dsn += "?"
			}
			dsn += "&_journal_mode=WAL&_busy_timeout=5000"
		}
		dialector = sqlite.Open(dsn)
	case "postgres":
		dialector = postgres.Open(cfg.DSN)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", cfg.Driver)
	}

	database, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := database.AutoMigrate(&Setting{}, &Device{}, &PendingCall{}); err != nil {
		return nil, fmt.Errorf("auto-migrate: %w", err)
	}

	return database, nil
}

func CleanupStaleState(database *gorm.DB) {
	result := database.Where("expires_at < ?", time.Now()).Delete(&Device{})
	if result.RowsAffected > 0 {
		slog.Info("cleaned up stale devices", "count", result.RowsAffected)
	}
	result = database.Where("expires_at < ?", time.Now()).Delete(&PendingCall{})
	if result.RowsAffected > 0 {
		slog.Info("cleaned up stale pending calls", "count", result.RowsAffected)
	}
}
