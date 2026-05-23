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
	"time"

	"github.com/AlchemillaHQ/Sentry/api"
	"github.com/AlchemillaHQ/Sentry/callmanager"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/push"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

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

	if cfg.Pprof.Addr != "" {
		go func() {
			slog.Info("pprof listening", "addr", cfg.Pprof.Addr)
			if err := http.ListenAndServe(cfg.Pprof.Addr, nil); err != nil {
				slog.Error("pprof failed", "error", err)
			}
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	database, err := db.Open(ctx, cfg.Database)
	if err != nil {
		slog.Error("database failed", "error", err)
		os.Exit(1)
	}

	encKey, err := database.GetOrCreateEncryptionKey(ctx)
	if err != nil {
		slog.Error("encryption key failed", "error", err)
		os.Exit(1)
	}
	box, err := secrets.NewBox(encKey)
	if err != nil {
		slog.Error("secrets failed", "error", err)
		os.Exit(1)
	}

	pushSender, err := push.NewDispatcher(cfg.Push)
	if err != nil {
		slog.Error("push init failed", "error", err)
		os.Exit(1)
	}

	pushSender.Start(ctx)

	stack, err := sipstack.New(cfg.SIP)
	if err != nil {
		slog.Error("SIP stack failed", "error", err)
		os.Exit(1)
	}
	defer stack.Close()

	registrar := sipstack.NewUpstreamRegistrar(stack)

	cm := callmanager.New(database, stack, registrar, pushSender, box)

	sipReady := make(chan struct{})
	slog.Info("starting SIP listeners...")
	go func() {
		close(sipReady)
		if err := stack.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			slog.Error("SIP stack error", "error", err)
			cancel()
		}
	}()

	<-sipReady

	database.CleanupStaleState(ctx)
	database.StartCleanupWorker(ctx)

	handler := api.NewHandler(database, registrar, box, stack, cfg.API.AuthKey)
	handler.SetCallManager(cm)
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

	go reregisterDevices(ctx, database, registrar, box)

	slog.Info("Sentry started")
	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	slog.Info("sending unregister to upstream PBXes...")
	registrar.UnregisterAll(shutdownCtx)

	cm.SendByeToAllBridgedCalls(shutdownCtx)

	srv.Shutdown(shutdownCtx)
	slog.Info("shutdown complete")
}

func reregisterDevices(ctx context.Context, database *db.Database, registrar *sipstack.UpstreamRegistrar, box *secrets.Box) {
	rows, err := database.Pool.Query(ctx, "SELECT device_id, upstream_host, upstream_port, upstream_transport, upstream_user, upstream_password, upstream_realm FROM devices")
	if err != nil {
		slog.Error("failed to query devices for re-registration", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var dID, host, transport, user, password, realm string
		var port int
		if err := rows.Scan(&dID, &host, &port, &transport, &user, &password, &realm); err != nil {
			slog.Error("failed to scan device", "error", err)
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
		pwBytes, err := box.Decrypt([]byte(password))
		if err != nil {
			slog.Error("decrypt password failed on startup", "device", dID, "error", err)
			continue
		}
		reg := &sipstack.UpstreamReg{
			DeviceID:  dID,
			User:      user,
			Host:      host,
			Port:      port,
			Transport: transport,
			Password:  string(pwBytes),
			Realm:     realm,
		}
		regCtx, regCancel := context.WithTimeout(ctx, 15*time.Second)
		if err := registrar.Register(regCtx, reg); err != nil {
			slog.Error("re-register on startup failed", "device", dID, "error", err)
		}
		regCancel()
	}
	slog.Info("startup re-registration complete")
}
