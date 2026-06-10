package logger

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestInit_DebugLevel(t *testing.T) {
	Init("debug", "", false)
	if zerolog.GlobalLevel() != zerolog.DebugLevel {
		t.Errorf("expected debug level, got %v", zerolog.GlobalLevel())
	}
}

func TestInit_WarnLevel(t *testing.T) {
	Init("warn", "", false)
	if zerolog.GlobalLevel() != zerolog.WarnLevel {
		t.Errorf("expected warn level, got %v", zerolog.GlobalLevel())
	}
}

func TestInit_ErrorLevel(t *testing.T) {
	Init("error", "", false)
	if zerolog.GlobalLevel() != zerolog.ErrorLevel {
		t.Errorf("expected error level, got %v", zerolog.GlobalLevel())
	}
}

func TestInit_DefaultLevel(t *testing.T) {
	Init("unknown", "", false)
	if zerolog.GlobalLevel() != zerolog.InfoLevel {
		t.Errorf("expected info level (default), got %v", zerolog.GlobalLevel())
	}
}

func TestInit_TerminalWriter(t *testing.T) {
	Init("info", "", true)
}

func TestInit_FileWriter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "logger-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	Init("debug", tmpDir, false)

	log.Info().Msg("test log entry")

	logPath := filepath.Join(tmpDir, "sentry.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("log file was not created")
	}
}

func TestInit_FileWriterWithTerminal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "logger-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	Init("debug", tmpDir, true)

	log.Info().Msg("test log entry")

	logPath := filepath.Join(tmpDir, "sentry.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("log file was not created")
	}
}

func TestInit_EmptyDataDir(t *testing.T) {
	Init("info", "", true)
}
