package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/worker/chunkstore"
)

func TestRegistrarRegisterHeartbeatReregister(t *testing.T) {
	var mu sync.Mutex
	var registers, heartbeats atomic.Int32
	known := false
	down := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(401)
			return
		}
		mu.Lock()
		isDown := down
		isKnown := known
		mu.Unlock()
		if isDown {
			w.WriteHeader(500)
			return
		}
		switch {
		case r.URL.Path == "/internal/workers/register" && r.Method == http.MethodPost:
			registers.Add(1)
			mu.Lock()
			known = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(api.RegisterResponse{HeartbeatIntervalMS: 20})
		case r.URL.Path == "/internal/workers/heartbeat" && r.Method == http.MethodPost:
			heartbeats.Add(1)
			var hb api.HeartbeatRequest
			_ = json.NewDecoder(r.Body).Decode(&hb)
			if !isKnown {
				w.WriteHeader(404)
				_ = json.NewEncoder(w).Encode(api.ErrorBody{Error: api.ErrorDetail{Code: api.CodeUnknownWorker}})
				return
			}
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	st, err := chunkstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testCfg(t.TempDir())
	cfg.ControllerURL = srv.URL
	cfg.HeartbeatInterval = 20 * time.Millisecond
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistrar(cfg, st, srv.Client(), log)
	reg.minBackoff = 5 * time.Millisecond
	reg.maxBackoff = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go reg.Run(ctx)

	waitUntil(t, time.Second, func() bool { return registers.Load() >= 1 && heartbeats.Load() >= 1 })

	mu.Lock()
	known = false
	mu.Unlock()
	waitUntil(t, time.Second, func() bool { return registers.Load() >= 2 })

	before := registers.Load()
	mu.Lock()
	down = true
	mu.Unlock()
	time.Sleep(80 * time.Millisecond)
	mu.Lock()
	down = false
	known = false
	mu.Unlock()
	waitUntil(t, time.Second, func() bool { return registers.Load() > before })
}

func waitUntil(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}
