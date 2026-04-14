package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlchemillaHQ/Difuse-B2BUA/api"
	"github.com/AlchemillaHQ/Difuse-B2BUA/callmanager"
	"github.com/AlchemillaHQ/Difuse-B2BUA/config"
	"github.com/AlchemillaHQ/Difuse-B2BUA/db"
	"github.com/AlchemillaHQ/Difuse-B2BUA/push"
	"github.com/AlchemillaHQ/Difuse-B2BUA/secrets"
	"github.com/AlchemillaHQ/Difuse-B2BUA/sipstack"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	// Setup logging
	var logLevel slog.Level
	switch cfg.Log.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	// Pprof
	if cfg.Pprof.Addr != "" {
		go func() {
			slog.Info("pprof listening", "addr", cfg.Pprof.Addr)
			if err := http.ListenAndServe(cfg.Pprof.Addr, nil); err != nil {
				slog.Error("pprof failed", "error", err)
			}
		}()
	}

	// Database
	database, err := db.Open(cfg.Database)
	if err != nil {
		slog.Error("database failed", "error", err)
		os.Exit(1)
	}

	// Encryption key (auto-generated, stored in DB)
	encKey, err := db.GetOrCreateEncryptionKey(database)
	if err != nil {
		slog.Error("encryption key failed", "error", err)
		os.Exit(1)
	}
	box, err := secrets.NewBox(encKey)
	if err != nil {
		slog.Error("secrets failed", "error", err)
		os.Exit(1)
	}

	// Push notifications
	pushSender, err := push.NewDispatcher(cfg.Push)
	if err != nil {
		slog.Error("push init failed", "error", err)
		os.Exit(1)
	}

	// SIP stack
	stack, err := sipstack.New(cfg.SIP)
	if err != nil {
		slog.Error("SIP stack failed", "error", err)
		os.Exit(1)
	}
	defer stack.Close()

	// Upstream registrar
	registrar := sipstack.NewUpstreamRegistrar(stack)
	defer registrar.StopAll()

	// Call manager (wires SIP handlers)
	_ = callmanager.New(database, stack, registrar, pushSender, box)

	// Context for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start SIP listeners
	go func() {
		if err := stack.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			slog.Error("SIP stack error", "error", err)
			cancel()
		}
	}()

	// REST API
	handler := api.NewHandler(database, registrar, box, stack)
	router := api.SetupRouter(handler)

	srv := &http.Server{
		Addr:    cfg.API.Addr,
		Handler: router,
	}

	go func() {
		slog.Info("REST API listening", "addr", cfg.API.Addr)
		var err error
		if cfg.API.TLSCert != "" && cfg.API.TLSKey != "" {
			err = srv.ListenAndServeTLS(cfg.API.TLSCert, cfg.API.TLSKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			slog.Error("API server error", "error", err)
			cancel()
		}
	}()

	// Re-register existing devices from DB on startup
	go func() {
		var devices []db.Device
		database.Find(&devices)
		for _, d := range devices {
			pwBytes, err := box.Decrypt(d.UpstreamPassword)
			if err != nil {
				slog.Error("decrypt password failed on startup", "device", d.DeviceID, "error", err)
				continue
			}
			reg := &sipstack.UpstreamReg{
				DeviceID:  d.DeviceID,
				User:      d.UpstreamUser,
				Host:      d.UpstreamHost,
				Port:      d.UpstreamPort,
				Transport: d.UpstreamTransport,
				Password:  string(pwBytes),
				Realm:     d.UpstreamRealm,
			}
			if err := registrar.Register(ctx, reg); err != nil {
				slog.Error("re-register on startup failed", "device", d.DeviceID, "error", err)
			}
		}
		slog.Info("startup re-registration complete")
	}()

	slog.Info("Difuse B2BUA started")
	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*1e9)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
}
