package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"objstore/internal/common/httpx"
	"objstore/internal/controller"
	"objstore/internal/controller/datapath"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := controller.LoadConfig()
	if err != nil {
		log.Error("configuration error", "err", err)
		os.Exit(1)
	}

	store, err := metadata.Open(cfg.DBPath)
	if err != nil {
		log.Error("failed to open metadata store", "err", err, "path", cfg.DBPath)
		os.Exit(1)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cleanup any stale UPLOADING records from interrupted previous runs
	cleaned, err := store.CleanupStaleUploading(ctx)
	if err != nil {
		log.Warn("cleanup stale uploading rows failed", "err", err)
	} else if cleaned > 0 {
		log.Info("cleaned stale uploading rows", "count", cleaned)
	}

	// Initialize worker registry and restore known workers from DB as SUSPECT
	reg := registry.New(store, cfg.SuspectAfter, cfg.DeadAfter, nil, log)
	if err := reg.LoadFromDB(ctx); err != nil {
		log.Error("failed to load known workers from DB", "err", err)
		os.Exit(1)
	}
	reg.StartSweeper(ctx)

	placer := placement.New(reg)
	wClient := workerclient.New(nil, cfg.ClusterToken)
	dp := datapath.New(cfg, store, reg, placer, wClient, log)
	ctrlServer := controller.NewServer(cfg, store, reg, placer, dp, log)

	srv := httpx.NewServer(cfg.ListenAddr, ctrlServer.Handler())

	errCh := make(chan error, 1)
	go func() {
		log.Info("controller listening", "addr", cfg.ListenAddr)
		errCh <- srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sig:
		log.Info("shutting down controller gracefully...")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
		}
	}

	cancel()
	shctx, shcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shcancel()

	if err := srv.Shutdown(shctx); err != nil {
		log.Error("server shutdown error", "err", err)
	}
}
