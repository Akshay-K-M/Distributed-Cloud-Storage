package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/worker/chunkstore"
)

func TestHeartbeatReportsQuarantined(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/workers/register" {
			_ = json.NewEncoder(w).Encode(api.RegisterResponse{HeartbeatIntervalMS: 20})
			return
		}
		var hb api.HeartbeatRequest
		_ = json.NewDecoder(r.Body).Decode(&hb)
		got.Store(hb)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	st, err := chunkstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hb-chunk")
	sum := shahex(data)
	if _, err := st.Put(context.Background(), "hb_0", sum, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	rc, _, err := st.Open("hb_0")
	if err != nil {
		t.Fatal(err)
	}
	path := rc.(*os.File).Name()
	rc.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	st.Scrub(context.Background(), 0)

	cfg := testCfg(t.TempDir())
	cfg.ControllerURL = srv.URL
	cfg.HeartbeatInterval = 20 * time.Millisecond
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistrar(cfg, st, srv.Client(), log)
	reg.minBackoff = 5 * time.Millisecond
	reg.maxBackoff = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go reg.Run(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		v := got.Load()
		if v == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		hb := v.(api.HeartbeatRequest)
		if hb.WorkerID == "worker1" && len(hb.CorruptChunks) == 1 && hb.CorruptChunks[0] == "hb_0" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("heartbeat never reported corrupt_chunks: %#v", got.Load())
}
