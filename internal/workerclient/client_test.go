// These tests verify worker HTTP client requests, responses, and failures.
package workerclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"objstore/internal/worker"
	"objstore/internal/worker/chunkstore"
)

func TestHTTPClientAgainstWorker(t *testing.T) {
	dir := t.TempDir()
	st, err := chunkstore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := worker.Config{
		Token:             "test-token",
		WorkerID:          "worker1",
		WorkerAddr:        "http://worker1:9000",
		DataDir:           dir,
		HeartbeatInterval: time.Second,
		ScrubInterval:     time.Hour,
		ScrubRateMBPS:     50,
		ChunkSize:         chunkstore.DefaultMaxChunk,
	}
	h := worker.Handler(cfg, st, worker.NewScrubber(st, time.Hour, 50, log), log)
	srv := httptest.NewServer(h)
	defer srv.Close()

	c := New(srv.Client(), "test-token")
	w := WorkerRef{ID: "worker1", Addr: srv.URL}
	ctx := context.Background()
	data := []byte("from-client")
	sum := sha256.Sum256(data)
	hexsum := hex.EncodeToString(sum[:])

	if err := c.PutChunk(ctx, w, "c_0", hexsum, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	body, size, stored, err := c.GetChunk(ctx, w, "c_0")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(body)
	body.Close()
	if size != int64(len(data)) || stored != hexsum || !bytes.Equal(got, data) {
		t.Fatalf("get mismatch size=%d sha=%s", size, stored)
	}
	inv, err := c.ListChunks(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.Chunks) != 1 || inv.Chunks[0].ChunkID != "c_0" {
		t.Fatalf("%+v", inv)
	}
	if err := c.Scrub(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteChunk(ctx, w, "c_0"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteChunk(ctx, w, "c_0"); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = c.GetChunk(ctx, w, "c_0")
	if err != ErrChunkNotFound {
		t.Fatalf("want not found, got %v", err)
	}
	err = c.PutChunk(ctx, w, "c_0", "00", int64(len(data)), bytes.NewReader(data))
	if err != ErrChecksumMismatch {
		t.Fatalf("want checksum mismatch, got %v", err)
	}
}

func TestFakeClient(t *testing.T) {
	f := NewFake()
	w := WorkerRef{ID: "w1", Addr: "http://w1"}
	ctx := context.Background()
	data := []byte("fake")
	sum := sha256.Sum256(data)
	hexsum := hex.EncodeToString(sum[:])
	if err := f.PutChunk(ctx, w, "c_0", hexsum, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	f.BeforeGet = func(WorkerRef, string) error { return ErrUnavailable }
	_, _, _, err := f.GetChunk(ctx, w, "c_0")
	if err != ErrUnavailable {
		t.Fatal(err)
	}
	f.BeforeGet = nil
	body, _, _, err := f.GetChunk(ctx, w, "c_0")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(body)
	if !bytes.Equal(got, data) {
		t.Fatal("mismatch")
	}
	f.Quarantine("w1", "c_0")
	_, _, _, err = f.GetChunk(ctx, w, "c_0")
	if err != ErrChunkNotFound {
		t.Fatal(err)
	}
}
