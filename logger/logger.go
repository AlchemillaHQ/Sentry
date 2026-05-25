package logger

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

func Init(logLevel string, dataDir string, isTerminal bool) {
	var level zerolog.Level
	switch logLevel {
	case "debug":
		level = zerolog.DebugLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	default:
		level = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339

	var writers []io.Writer

	if isTerminal {
		writers = append(writers, zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "15:04:05",
		})
	} else {
		writers = append(writers, os.Stdout)
	}

	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0755); err == nil {
			logPath := filepath.Join(dataDir, "sentry.log")
			writers = append(writers, &lumberjack.Logger{
				Filename:   logPath,
				MaxSize:    50, // megabytes
				MaxBackups: 3,
				MaxAge:     28,   // days
				Compress:   true, // disabled by default
			})
		}
	}

	multi := zerolog.MultiLevelWriter(writers...)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()
}
