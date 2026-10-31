// These tests verify deletion cleanup and orphan garbage collection.
package reconciler_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/checksum"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/reconciler"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

func TestGarbageCollector(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gc_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	reg := registry.New(store, 6*time.Second, 15*time.Second, nil, nil)
	fakeClient := workerclient.NewFake()

	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w1", Address: "http://w1:9000"})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w2", Address: "http://w2:9000"})

	gc := reconciler.NewGarbageCollector(store, reg, fakeClient, nil)

	data := []byte("gc test chunk")
	sha := checksum.SHA256Hex(data)

	objID, _ := store.BeginUpload(ctx, "delete.txt", "text/plain", false)
	chunkID := objID + "_0"
	_, _ = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       objID,
		Name:           "delete.txt",
		Size:           int64(len(data)),
		ChecksumSHA256: sha,
		ChunkCount:     1,
		Chunks: []metadata.ChunkCommit{
			{ChunkID: chunkID, Index: 0, Size: int64(len(data)), SHA256: sha, Replicas: []string{"w1", "w2"}},
		},
	})

	_ = fakeClient.PutChunk(ctx, workerclient.WorkerRef{ID: "w1"}, chunkID, sha, int64(len(data)), bytes.NewReader(data))
	_ = fakeClient.PutChunk(ctx, workerclient.WorkerRef{ID: "w2"}, chunkID, sha, int64(len(data)), bytes.NewReader(data))

	// Mark deleting
	_ = store.MarkDeleting(ctx, objID)

	// Run GC pass
	if err := gc.Run(ctx); err != nil {
		t.Fatalf("gc.Run failed: %v", err)
	}

	// Chunks on both workers should be deleted
	if _, _, _, err := fakeClient.GetChunk(ctx, workerclient.WorkerRef{ID: "w1"}, chunkID); err != workerclient.ErrChunkNotFound {
		t.Errorf("expected chunk to be deleted on w1, got: %v", err)
	}
	if _, _, _, err := fakeClient.GetChunk(ctx, workerclient.WorkerRef{ID: "w2"}, chunkID); err != workerclient.ErrChunkNotFound {
		t.Errorf("expected chunk to be deleted on w2, got: %v", err)
	}

	// Object row should be completely deleted from DB
	deletingObjs, _ := store.ListDeletingObjects(ctx)
	if len(deletingObjs) != 0 {
		t.Errorf("expected 0 deleting objects remaining in DB, got %d", len(deletingObjs))
	}
}
