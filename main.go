package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AlchemillaHQ/Sentry/api"
	"github.com/AlchemillaHQ/Sentry/callmanager"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/logger"
	"github.com/AlchemillaHQ/Sentry/push"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/rs/zerolog/log"
)

var (
	Version   = "v0.0.1"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	dataDir := flag.String("data-dir", "./data", "Directory for logs and persistent data")
	resetDB := flag.Bool("reset-db", false, "Reset the database and exit")
	showVersion := flag.Bool("version", false, "Show version information")
	flag.Parse()

	if *showVersion {
		fmt.Printf("Sentry %s\nCommit: %s\nBuildTime: %s\n", Version, GitCommit, BuildTime)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	// Initialize new zerolog/lumberjack logger
	logger.Init(cfg.Log.Level, *dataDir, true)

	if cfg.API.JWTSecret == "" || cfg.API.JWTSecret == "CHANGE_ME_FOR_DASHBOARD" {
		log.Warn().Msg("JWT secret is not configured or uses the default example value — set api.jwt_secret in config.yaml")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	database, err := db.Open(ctx, cfg.Database)
	if err != nil {
		log.Error().Err(err).Msg("database failed")
		os.Exit(1)
	}

	if *resetDB {
		log.Info().Msg("resetting database...")
		if err := database.Reset(ctx); err != nil {
			log.Error().Err(err).Msg("database reset failed")
			os.Exit(1)
		}
		log.Info().Msg("database reset successful")
		return
	}

	encKey, err := database.GetOrCreateEncryptionKey(ctx, cfg.EncryptionKey)
	if err != nil {
		log.Error().Err(err).Msg("encryption key failed")
		os.Exit(1)
	}
	box, err := secrets.NewBox(encKey)
	if err != nil {
		log.Error().Err(err).Msg("secrets failed")
		os.Exit(1)
	}

	pushSender, err := push.NewDispatcher(cfg.Push)
	if err != nil {
		log.Error().Err(err).Msg("push init failed")
		os.Exit(1)
	}

	stack, err := sipstack.New(cfg.SIP)
	if err != nil {
		log.Error().Err(err).Msg("SIP stack failed")
		os.Exit(1)
	}
	defer stack.Close()

	registrar := sipstack.NewUpstreamRegistrar(stack, cfg.Registrar)

	cm := callmanager.New(database, stack, registrar, pushSender, box)
	handler := api.NewHandler(database, registrar, box, stack, cfg.API)
	handler.SetCallManager(cm)

	pushSender.OnDeadToken(func(call push.CallPush) {
		log.Warn().
			Str("call_id", call.CallID).
			Str("device", call.DeviceID).
			Str("platform", call.Platform).
			Msg("push token invalid, disabling device")
		disableCtx, disableCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer disableCancel()
		if _, err := handler.DisableDeviceByID(disableCtx, call.DeviceID, "invalid_push_token"); err != nil {
			log.Error().Err(err).Str("device", call.DeviceID).Str("call_id", call.CallID).Msg("dead-token disable failed")
		}
	})
	pushSender.Start(ctx)

	sipReady := make(chan struct{})
	log.Info().Msg("starting SIP listeners...")
	go func() {
		close(sipReady)
		if err := stack.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("SIP stack error")
			cancel()
		}
	}()

	<-sipReady

	database.CleanupStaleState(ctx)
	database.CleanupJunkDevices(ctx)
	database.StartCleanupWorker(ctx, handler.RetirePrunedDevices)

	if err := database.BootstrapUsers(ctx, cfg.Admin.BootstrapUsers); err != nil {
		log.Error().Err(err).Msg("bootstrap users failed")
	}

	router := api.SetupRouter(handler, cfg)
	ServeSPA(router)

	srv := &http.Server{
		Addr:    cfg.API.Addr,
		Handler: router,
	}

	go func() {
		log.Info().Str("addr", cfg.API.Addr).Msg("REST API listening")
		var err error
		if cfg.API.TLSCert != "" && cfg.API.TLSKey != "" {
			err = srv.ListenAndServeTLS(cfg.API.TLSCert, cfg.API.TLSKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("API server error")
			cancel()
		}
	}()

	go reconcileRegistrations(ctx, database, handler)

	log.Info().Str("version", Version).Msg("Sentry started")
	<-ctx.Done()
	log.Info().Msg("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	log.Info().Msg("sending unregister to upstream PBXes...")
	registrar.UnregisterAll(shutdownCtx)

	cm.SendByeToAllBridgedCalls(shutdownCtx)

	srv.Shutdown(shutdownCtx)
	log.Info().Msg("shutdown complete")
}

func reconcileRegistrations(ctx context.Context, database *db.Database, handler *api.Handler) {
	rows, err := database.Pool.Query(ctx, "SELECT device_id FROM devices WHERE disabled = false")
	if err != nil {
		log.Error().Err(err).Msg("failed to query devices for re-registration")
		return
	}
	defer rows.Close()

	const maxConcurrent = 50
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup
	for rows.Next() {
		var dID string
		if err := rows.Scan(&dID); err != nil {
			log.Error().Err(err).Msg("failed to scan device")
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(deviceID string) {
			defer func() { <-sem }()
			defer wg.Done()
			if _, err := handler.EnsureEnabledRegistration(ctx, deviceID, "startup"); err != nil {
				log.Error().Err(err).Str("device", deviceID).Msg("re-register on startup failed")
			}
		}(dID)
	}
	wg.Wait()
	log.Info().Msg("startup registration reconciliation queued")
}
