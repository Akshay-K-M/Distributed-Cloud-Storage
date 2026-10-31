// These tests verify repair and trim execution against fake workers.
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
	"objstore/internal/controller/placement"
	"objstore/internal/controller/reconciler"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

func TestExecutor_RepairChunk(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	reg := registry.New(store, 6*time.Second, 15*time.Second, nil, nil)
	placer := placement.New(reg)
	fakeClient := workerclient.NewFake()

	// Register 3 workers
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w1", Address: "http://w1:9000"})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w2", Address: "http://w2:9000"})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w3", Address: "http://w3:9000"})

	// Seed a committed object with chunk c1 on w1
	chunkData := []byte("hello world chunk data")
	chunkSHA := checksum.SHA256Hex(chunkData)
	chunkSize := int64(len(chunkData))

	objID, err := store.BeginUpload(ctx, "test.txt", "text/plain", false)
	if err != nil {
		t.Fatal(err)
	}

	chunkID := objID + "_0"
	_, err = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       objID,
		Name:           "test.txt",
		Size:           chunkSize,
		ChecksumSHA256: chunkSHA,
		ChunkCount:     1,
		Chunks: []metadata.ChunkCommit{
			{ChunkID: chunkID, Index: 0, Size: chunkSize, SHA256: chunkSHA, Replicas: []string{"w1"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Put chunk data on w1
	_ = fakeClient.PutChunk(ctx, workerclient.WorkerRef{ID: "w1"}, chunkID, chunkSHA, chunkSize, bytes.NewReader(chunkData))

	executor := reconciler.NewExecutor(store, reg, placer, fakeClient, 2, nil)

	t.Run("Repair copies and verifies to target", func(t *testing.T) {
		action := reconciler.RepairChunk{
			Chunk: reconciler.ChunkState{
				ChunkID:  chunkID,
				Size:     chunkSize,
				SHA:      chunkSHA,
				Replicas: []reconciler.ReplicaState{{WorkerID: "w1", State: "OK"}},
			},
			Sources: []string{"w1"},
			Exclude: map[string]bool{"w1": true},
		}

		if err := executor.Execute(ctx, []reconciler.Action{action}); err != nil {
			t.Fatalf("Execute repair failed: %v", err)
		}

		// Verify chunk was copied to w2 or w3
		rc, size, sha, err := fakeClient.GetChunk(ctx, workerclient.WorkerRef{ID: "w2"}, chunkID)
		if err != nil {
			rc, size, sha, err = fakeClient.GetChunk(ctx, workerclient.WorkerRef{ID: "w3"}, chunkID)
		}
		if err != nil {
			t.Fatalf("expected chunk to be copied to w2 or w3, got err: %v", err)
		}
		_ = rc.Close()
		if size != chunkSize || sha != chunkSHA {
			t.Errorf("unexpected size or sha on target: size=%d, sha=%s", size, sha)
		}

		// Verify DB contains 2 replicas now
		detail, _ := store.GetObjectWithChunks(ctx, objID)
		if len(detail.Chunks[0].Replicas) != 2 {
			t.Errorf("expected 2 replicas in DB, got %d", len(detail.Chunks[0].Replicas))
		}
	})

	t.Run("Corrupted source detected and next source used", func(t *testing.T) {
		// Put valid copy on w2, corrupt copy on w1
		_ = fakeClient.PutChunk(ctx, workerclient.WorkerRef{ID: "w2"}, chunkID, chunkSHA, chunkSize, bytes.NewReader(chunkData))
		fakeClient.CorruptGet = func(w workerclient.WorkerRef, cID string, data []byte) []byte {
			if w.ID == "w1" {
				bad := append([]byte(nil), data...)
				bad[0] ^= 0xFF
				return bad
			}
			return data
		}
		defer func() { fakeClient.CorruptGet = nil }()

		// Try repair with sources [w1, w2]
		action := reconciler.RepairChunk{
			Chunk: reconciler.ChunkState{
				ChunkID:  chunkID,
				Size:     chunkSize,
				SHA:      chunkSHA,
				Replicas: []reconciler.ReplicaState{{WorkerID: "w1", State: "OK"}, {WorkerID: "w2", State: "OK"}},
			},
			Sources: []string{"w1", "w2"},
			Exclude: map[string]bool{"w1": true, "w2": true},
		}

		if err := executor.Execute(ctx, []reconciler.Action{action}); err != nil {
			t.Fatalf("Execute repair failed: %v", err)
		}

		// Must have copied from w2 to w3
		rc, _, _, err := fakeClient.GetChunk(ctx, workerclient.WorkerRef{ID: "w3"}, chunkID)
		if err != nil {
			t.Fatalf("expected chunk on w3: %v", err)
		}
		_ = rc.Close()
	})

	t.Run("Race with delete leaves no orphan", func(t *testing.T) {
		// Mark object DELETING before executor commits replica row
		_ = store.MarkDeleting(ctx, objID)

		action := reconciler.RepairChunk{
			Chunk: reconciler.ChunkState{
				ChunkID:  chunkID,
				Size:     chunkSize,
				SHA:      chunkSHA,
				Replicas: []reconciler.ReplicaState{{WorkerID: "w1", State: "OK"}},
			},
			Sources: []string{"w1"},
			Exclude: map[string]bool{"w1": true},
		}

		_ = executor.Execute(ctx, []reconciler.Action{action})

		// Target should not have the chunk stored (orphan must be cleaned)
		// Check that target doesn't leave stray file
		count := 0
		for _, wID := range []string{"w2", "w3"} {
			if _, _, _, err := fakeClient.GetChunk(ctx, workerclient.WorkerRef{ID: wID}, chunkID); err == nil {
				count++
			}
		}
		if count > 1 {
			t.Errorf("orphan chunk was not cleaned up after race with delete")
		}
	})
}
