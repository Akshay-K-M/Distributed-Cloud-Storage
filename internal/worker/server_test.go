// These tests cover worker HTTP endpoints, authentication, and chunk streaming.
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/worker/chunkstore"
)

func shahex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func testCfg(dir string) Config {
	return Config{
		Token:             "test-token",
		WorkerID:          "worker1",
		WorkerAddr:        "http://worker1:9000",
		Listen:            ":9000",
		ControllerURL:     "http://controller:8080",
		DataDir:           dir,
		HeartbeatInterval: 2 * time.Second,
		ScrubInterval:     time.Hour,
		ScrubRateMBPS:     50,
		ChunkSize:         chunkstore.DefaultMaxChunk,
	}
}

func testServer(t *testing.T) (http.Handler, *chunkstore.Store, *Scrubber) {
	t.Helper()
	dir := t.TempDir()
	st, err := chunkstore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testCfg(dir)
	sc := NewScrubber(st, time.Hour, 50, log)
	return Handler(cfg, st, sc, log), st, sc
}

func doAuth(h http.Handler, method, path, token string, body []byte, hdr map[string]string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		req.ContentLength = int64(len(body))
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestHealthzOpen(t *testing.T) {
	h, _, _ := testServer(t)
	rr := doAuth(h, http.MethodGet, "/healthz", "", nil, nil)
	if rr.Code != 200 {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.Bytes())
	}
	var z api.Healthz
	if err := json.Unmarshal(rr.Body.Bytes(), &z); err != nil {
		t.Fatal(err)
	}
	if z.Status != "ok" || z.WorkerID != "worker1" {
		t.Fatalf("%+v", z)
	}
}

func TestBearerRequired(t *testing.T) {
	h, _, _ := testServer(t)
	rr := doAuth(h, http.MethodGet, "/internal/workers/worker1/chunks", "", nil, nil)
	if rr.Code != 401 {
		t.Fatalf("missing token: %d", rr.Code)
	}
	var env api.ErrorBody
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != api.CodeUnauthorized {
		t.Fatalf("code %s", env.Error.Code)
	}
	rr = doAuth(h, http.MethodGet, "/internal/workers/worker1/chunks", "wrong", nil, nil)
	if rr.Code != 401 {
		t.Fatalf("wrong token: %d", rr.Code)
	}
}

func TestPutGetDeleteInventory(t *testing.T) {
	h, _, _ := testServer(t)
	data := []byte("hello-chunk")
	sum := shahex(data)
	hdr := map[string]string{"X-Chunk-SHA256": sum, "Content-Type": "application/octet-stream"}
	rr := doAuth(h, http.MethodPut, "/internal/chunks/obj_0", "test-token", data, hdr)
	if rr.Code != 201 {
		t.Fatalf("put new %d %s", rr.Code, rr.Body.Bytes())
	}
	rr = doAuth(h, http.MethodPut, "/internal/chunks/obj_0", "test-token", data, hdr)
	if rr.Code != 200 {
		t.Fatalf("overwrite %d", rr.Code)
	}

	rr = doAuth(h, http.MethodGet, "/internal/chunks/obj_0", "test-token", nil, nil)
	if rr.Code != 200 {
		t.Fatalf("get %d", rr.Code)
	}
	if !bytes.Equal(rr.Body.Bytes(), data) {
		t.Fatal("body mismatch")
	}
	if rr.Header().Get("X-Chunk-SHA256") != sum {
		t.Fatal("sha header")
	}
	if rr.Header().Get("Accept-Ranges") != "none" {
		t.Fatal("accept-ranges")
	}

	rr = doAuth(h, http.MethodGet, "/internal/workers/worker1/chunks", "test-token", nil, nil)
	if rr.Code != 200 {
		t.Fatal(rr.Body.String())
	}
	var inv api.Inventory
	if err := json.Unmarshal(rr.Body.Bytes(), &inv); err != nil {
		t.Fatal(err)
	}
	if inv.WorkerID != "worker1" || len(inv.Chunks) != 1 || inv.Chunks[0].ChunkID != "obj_0" {
		t.Fatalf("%+v", inv)
	}

	rr = doAuth(h, http.MethodDelete, "/internal/chunks/obj_0", "test-token", nil, nil)
	if rr.Code != 204 {
		t.Fatalf("delete %d", rr.Code)
	}
	rr = doAuth(h, http.MethodDelete, "/internal/chunks/obj_0", "test-token", nil, nil)
	if rr.Code != 204 {
		t.Fatalf("idempotent delete %d", rr.Code)
	}
	rr = doAuth(h, http.MethodGet, "/internal/chunks/obj_0", "test-token", nil, nil)
	if rr.Code != 404 {
		t.Fatalf("missing %d", rr.Code)
	}
}

func TestScrubHTTPQuarantine(t *testing.T) {
	h, st, sc := testServer(t)
	data := bytes.Repeat([]byte("n"), 512)
	sum := shahex(data)
	if _, err := st.Put(context.Background(), "bad_0", sum, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	rc, _, err := st.Open("bad_0")
	if err != nil {
		t.Fatal(err)
	}
	found := rc.(*os.File).Name()
	rc.Close()
	b, err := os.ReadFile(found)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(found, b, 0o644); err != nil {
		t.Fatal(err)
	}

	rr := doAuth(h, http.MethodPost, "/internal/scrub", "test-token", nil, nil)
	if rr.Code != 202 {
		t.Fatalf("scrub %d", rr.Code)
	}
	sc.runOnce(context.Background())

	rr = doAuth(h, http.MethodGet, "/internal/chunks/bad_0", "test-token", nil, nil)
	if rr.Code != 404 {
		t.Fatalf("quarantined get %d", rr.Code)
	}
	rr = doAuth(h, http.MethodGet, "/internal/workers/worker1/chunks", "test-token", nil, nil)
	var inv api.Inventory
	_ = json.Unmarshal(rr.Body.Bytes(), &inv)
	if len(inv.Quarantined) != 1 || inv.Quarantined[0] != "bad_0" {
		t.Fatalf("inv %+v", inv)
	}
}

func TestChecksumMismatchHTTP(t *testing.T) {
	h, _, _ := testServer(t)
	data := []byte("abc")
	hdr := map[string]string{"X-Chunk-SHA256": "00", "Content-Type": "application/octet-stream"}
	rr := doAuth(h, http.MethodPut, "/internal/chunks/obj_0", "test-token", data, hdr)
	if rr.Code != 400 {
		t.Fatalf("status %d", rr.Code)
	}
	var env api.ErrorBody
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.Error.Code != api.CodeChecksumMismatch {
		t.Fatalf("code %s", env.Error.Code)
	}
}

func TestMismatchedWorkerIDInventory(t *testing.T) {
	h, _, _ := testServer(t)
	rr := doAuth(h, http.MethodGet, "/internal/workers/wrong_worker/chunks", "test-token", nil, nil)
	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var env api.ErrorBody
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.Error.Code != api.CodeUnknownWorker {
		t.Fatalf("expected UNKNOWN_WORKER, got %s", env.Error.Code)
	}
}

func TestOversizedPut(t *testing.T) {
	h, _, _ := testServer(t)
	data := make([]byte, chunkstore.DefaultMaxChunk+1)
	hdr := map[string]string{
		"X-Chunk-SHA256": shahex(data),
		"Content-Type":   "application/octet-stream",
	}
	rr := doAuth(h, http.MethodPut, "/internal/chunks/big_0", "test-token", data, hdr)
	if rr.Code != 400 {
		t.Fatalf("expected 400 for oversized chunk, got %d", rr.Code)
	}
}

func TestHeadChunk(t *testing.T) {
	h, _, _ := testServer(t)
	data := []byte("head-test-data")
	sum := shahex(data)
	hdr := map[string]string{"X-Chunk-SHA256": sum, "Content-Type": "application/octet-stream"}
	rr := doAuth(h, http.MethodPut, "/internal/chunks/head_0", "test-token", data, hdr)
	if rr.Code != 201 {
		t.Fatalf("put failed: %d", rr.Code)
	}

	rr = doAuth(h, http.MethodHead, "/internal/chunks/head_0", "test-token", nil, nil)
	if rr.Code != 200 {
		t.Fatalf("head failed: %d", rr.Code)
	}
	if rr.Header().Get("X-Chunk-SHA256") != sum {
		t.Fatalf("head sha mismatch: %s", rr.Header().Get("X-Chunk-SHA256"))
	}
	if rr.Header().Get("Content-Length") != strconv.Itoa(len(data)) {
		t.Fatalf("head content length mismatch: %s", rr.Header().Get("Content-Length"))
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("head should return empty body, got %d bytes", rr.Body.Len())
	}
}
