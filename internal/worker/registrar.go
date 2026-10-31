// This file registers workers and sends periodic heartbeats to the controller.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
	"objstore/internal/worker/chunkstore"
)

// Registrar registers with the controller and sends heartbeats.
type Registrar struct {
	cfg               Config
	store             *chunkstore.Store
	client            *http.Client
	log               *slog.Logger
	minBackoff        time.Duration
	maxBackoff        time.Duration
	heartbeatInterval time.Duration
}

// NewRegistrar constructs a registrar with 1s–10s backoff.
func NewRegistrar(cfg Config, store *chunkstore.Store, client *http.Client, log *slog.Logger) *Registrar {
	if client == nil {
		client = httpx.NewHTTPClient()
	}
	return &Registrar{
		cfg:               cfg,
		store:             store,
		client:            client,
		log:               log,
		minBackoff:        time.Second,
		maxBackoff:        10 * time.Second,
		heartbeatInterval: cfg.HeartbeatInterval,
	}
}

// Run blocks until ctx is cancelled. Never panics on controller failure.
func (r *Registrar) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.registerLoop(ctx); err != nil {
			return
		}
		if err := r.heartbeatLoop(ctx); err != nil {
			return
		}
	}
}

func (r *Registrar) registerLoop(ctx context.Context) error {
	backoff := r.minBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.register(ctx); err != nil {
			r.log.Warn("register failed", "err", err)
			if err := sleepJitter(ctx, backoff); err != nil {
				return err
			}
			backoff *= 2
			if backoff > r.maxBackoff {
				backoff = r.maxBackoff
			}
			continue
		}
		r.log.Info("registered with controller", "url", r.cfg.ControllerURL)
		return nil
	}
}

func (r *Registrar) heartbeatLoop(ctx context.Context) error {
	interval := r.heartbeatInterval
	if interval <= 0 {
		interval = r.cfg.HeartbeatInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			st, err := r.heartbeat(ctx)
			if err != nil {
				r.log.Warn("heartbeat failed", "err", err)
				continue
			}
			if st == http.StatusNotFound {
				r.log.Warn("controller returned UNKNOWN_WORKER; re-registering")
				return nil
			}
		}
	}
}

func (r *Registrar) register(ctx context.Context) error {
	used, count, capBytes := r.store.Stats()
	body := api.RegisterRequest{
		WorkerID:      r.cfg.WorkerID,
		Address:       r.cfg.WorkerAddr,
		CapacityBytes: capBytes,
		UsedBytes:     used,
		ChunkCount:    count,
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := r.doJSON(ctx, http.MethodPost, r.cfg.ControllerURL+"/internal/workers/register", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("register status %d: %s", resp.StatusCode, b)
	}
	var out api.RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err == nil && out.HeartbeatIntervalMS > 0 {
		r.heartbeatInterval = time.Duration(out.HeartbeatIntervalMS) * time.Millisecond
	}
	return nil
}

func (r *Registrar) heartbeat(ctx context.Context) (int, error) {
	used, count, capBytes := r.store.Stats()
	_, quarantined := r.store.Inventory()
	body := api.HeartbeatRequest{
		WorkerID:      r.cfg.WorkerID,
		UsedBytes:     used,
		CapacityBytes: capBytes,
		ChunkCount:    count,
		CorruptChunks: quarantined,
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := r.doJSON(ctx, http.MethodPost, r.cfg.ControllerURL+"/internal/workers/heartbeat", body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
		return resp.StatusCode, nil
	}
	return resp.StatusCode, fmt.Errorf("heartbeat status %d", resp.StatusCode)
}

func (r *Registrar) doJSON(ctx context.Context, method, url string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.cfg.Token)
	return r.client.Do(req)
}

func sleepJitter(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = time.Millisecond
	}
	j := time.Duration(rand.Int63n(int64(d) + 1))
	wait := d/2 + j/2
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
