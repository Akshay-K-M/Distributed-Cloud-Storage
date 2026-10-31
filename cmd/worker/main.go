// The worker process stores chunks locally and registers with the controller.
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
	"objstore/internal/worker"
	"objstore/internal/worker/chunkstore"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := worker.LoadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	store, err := chunkstore.Open(cfg.DataDir)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}

	scrubber := worker.NewScrubber(store, cfg.ScrubInterval, cfg.ScrubRateMBPS, log)
	h := worker.Handler(cfg, store, scrubber, log)
	srv := httpx.NewServer(cfg.Listen, h)
	client := httpx.NewHTTPClient()
	reg := worker.NewRegistrar(cfg, store, client, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go scrubber.Run(ctx)
	go reg.Run(ctx)

	errCh := make(chan error, 1)
	go func() {
		log.Info("worker listening", "addr", cfg.Listen, "id", cfg.WorkerID)
		errCh <- srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
		}
	}

	cancel()
	shctx, shcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shcancel()
	if err := srv.Shutdown(shctx); err != nil {
		log.Error("shutdown", "err", err)
	}
}
