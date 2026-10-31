// These tests verify SQLite initialization and object lifecycle persistence.
package metadata_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/metadata"
)

func newTestStore(t *testing.T) metadata.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func TestStore_CommitHappyPath(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	objID, err := store.BeginUpload(ctx, "photos/cat.jpg", "image/jpeg", false)
	if err != nil {
		t.Fatalf("BeginUpload failed: %v", err)
	}
	if objID == "" {
		t.Fatalf("expected non-empty objectID")
	}

	commit := metadata.CommitInput{
		ObjectID:       objID,
		Name:           "photos/cat.jpg",
		Size:           2097152,
		ChecksumSHA256: "abcdef123456",
		ChunkCount:     2,
		ContentType:    "image/jpeg",
		CustomMetadata: map[string]string{"owner": "alice"},
		Overwrite:      false,
		Chunks: []metadata.ChunkCommit{
			{
				ChunkID:  objID + "_0",
				Index:    0,
				Size:     1048576,
				SHA256:   "hash0",
				Replicas: []string{"worker1", "worker2"},
			},
			{
				ChunkID:  objID + "_1",
				Index:    1,
				Size:     1048576,
				SHA256:   "hash1",
				Replicas: []string{"worker2", "worker3"},
			},
		},
	}

	obj, err := store.CommitObject(ctx, commit)
	if err != nil {
		t.Fatalf("CommitObject failed: %v", err)
	}
	if obj.ObjectID != objID || obj.Name != "photos/cat.jpg" || obj.Size != 2097152 {
		t.Errorf("unexpected object fields: %+v", obj)
	}
	if obj.CustomMetadata["owner"] != "alice" {
		t.Errorf("unexpected custom metadata: %+v", obj.CustomMetadata)
	}

	// Verify GetObject
	got, err := store.GetObject(ctx, objID)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if got.ObjectID != objID || got.Name != "photos/cat.jpg" {
		t.Errorf("got: %+v", got)
	}

	// Verify GetObjectWithChunks
	detail, err := store.GetObjectWithChunks(ctx, objID)
	if err != nil {
		t.Fatalf("GetObjectWithChunks failed: %v", err)
	}
	if len(detail.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(detail.Chunks))
	}
	if len(detail.Chunks[0].Replicas) != 2 {
		t.Fatalf("expected 2 replicas on chunk 0, got %d", len(detail.Chunks[0].Replicas))
	}
}

func TestStore_NameConflictAndOverwrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	// 1. Upload first object
	id1, err := store.BeginUpload(ctx, "file.txt", "text/plain", false)
	if err != nil {
		t.Fatalf("BeginUpload id1: %v", err)
	}
	_, err = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       id1,
		Name:           "file.txt",
		Size:           10,
		ChecksumSHA256: "hash1",
		ChunkCount:     1,
		ContentType:    "text/plain",
		Chunks: []metadata.ChunkCommit{
			{ChunkID: id1 + "_0", Index: 0, Size: 10, SHA256: "hash1", Replicas: []string{"worker1"}},
		},
	})
	if err != nil {
		t.Fatalf("Commit id1: %v", err)
	}

	// 2. Begin upload with same name and overwrite=false should fail early
	_, err = store.BeginUpload(ctx, "file.txt", "text/plain", false)
	if err != metadata.ErrNameConflict {
		t.Fatalf("expected ErrNameConflict, got: %v", err)
	}

	// 3. Begin upload with same name and overwrite=true should succeed
	id2, err := store.BeginUpload(ctx, "file.txt", "text/plain", true)
	if err != nil {
		t.Fatalf("BeginUpload id2 overwrite: %v", err)
	}

	// 4. Commit id2 with Overwrite=true
	_, err = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       id2,
		Name:           "file.txt",
		Size:           20,
		ChecksumSHA256: "hash2",
		ChunkCount:     1,
		ContentType:    "text/plain",
		Overwrite:      true,
		Chunks: []metadata.ChunkCommit{
			{ChunkID: id2 + "_0", Index: 0, Size: 20, SHA256: "hash2", Replicas: []string{"worker2"}},
		},
	})
	if err != nil {
		t.Fatalf("Commit id2 overwrite: %v", err)
	}

	// 5. Old object id1 should now be DELETING (GetObject should return ErrNotFound)
	_, err = store.GetObject(ctx, id1)
	if err != metadata.ErrNotFound {
		t.Fatalf("expected id1 to return ErrNotFound, got: %v", err)
	}

	// 6. New object id2 should be COMMITTED
	obj2, err := store.GetObject(ctx, id2)
	if err != nil {
		t.Fatalf("GetObject id2 failed: %v", err)
	}
	if obj2.Size != 20 {
		t.Errorf("expected size 20, got %d", obj2.Size)
	}
}

func TestStore_ConcurrentCommitsSameName(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	// Two uploads started without overwrite check colliding
	id1, err := store.BeginUpload(ctx, "race.txt", "text/plain", true)
	if err != nil {
		t.Fatalf("BeginUpload 1: %v", err)
	}
	id2, err := store.BeginUpload(ctx, "race.txt", "text/plain", true)
	if err != nil {
		t.Fatalf("BeginUpload 2: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, errs[0] = store.CommitObject(ctx, metadata.CommitInput{
			ObjectID:       id1,
			Name:           "race.txt",
			Size:           100,
			ChecksumSHA256: "h1",
			ChunkCount:     1,
			Overwrite:      false,
			Chunks: []metadata.ChunkCommit{
				{ChunkID: id1 + "_0", Index: 0, Size: 100, SHA256: "h1", Replicas: []string{"w1"}},
			},
		})
	}()

	go func() {
		defer wg.Done()
		_, errs[1] = store.CommitObject(ctx, metadata.CommitInput{
			ObjectID:       id2,
			Name:           "race.txt",
			Size:           200,
			ChecksumSHA256: "h2",
			ChunkCount:     1,
			Overwrite:      false,
			Chunks: []metadata.ChunkCommit{
				{ChunkID: id2 + "_0", Index: 0, Size: 200, SHA256: "h2", Replicas: []string{"w2"}},
			},
		})
	}()

	wg.Wait()

	successCount := 0
	conflictCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		} else if e == metadata.ErrNameConflict {
			conflictCount++
		} else {
			t.Errorf("unexpected error: %v", e)
		}
	}

	if successCount != 1 || conflictCount != 1 {
		t.Errorf("expected exactly 1 success and 1 conflict, got %d successes and %d conflicts", successCount, conflictCount)
	}
}

func TestStore_CleanupStaleUploading(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, err := store.BeginUpload(ctx, "stale1.txt", "text/plain", false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.BeginUpload(ctx, "stale2.txt", "text/plain", false)
	if err != nil {
		t.Fatal(err)
	}

	cleaned, err := store.CleanupStaleUploading(ctx)
	if err != nil {
		t.Fatalf("CleanupStaleUploading: %v", err)
	}
	if cleaned != 2 {
		t.Errorf("expected 2 cleaned, got %d", cleaned)
	}
}

func TestStore_MarkDeleting(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	id, err := store.BeginUpload(ctx, "delete_me.txt", "text/plain", false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       id,
		Name:           "delete_me.txt",
		Size:           50,
		ChecksumSHA256: "h",
		ChunkCount:     1,
		Chunks: []metadata.ChunkCommit{
			{ChunkID: id + "_0", Index: 0, Size: 50, SHA256: "h", Replicas: []string{"w1"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.MarkDeleting(ctx, id); err != nil {
		t.Fatalf("MarkDeleting failed: %v", err)
	}

	// Once marked DELETING, GetObject returns ErrNotFound
	if _, err := store.GetObject(ctx, id); err != metadata.ErrNotFound {
		t.Errorf("expected ErrNotFound after MarkDeleting, got: %v", err)
	}

	// Repeated MarkDeleting returns ErrNotFound
	if err := store.MarkDeleting(ctx, id); err != metadata.ErrNotFound {
		t.Errorf("expected second MarkDeleting to return ErrNotFound, got: %v", err)
	}
}

func TestStore_WorkerAndReplicaHelpers(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UnixMilli()
	w := metadata.WorkerRow{
		WorkerID:         "worker1",
		Address:          "http://worker1:9000",
		Status:           "ALIVE",
		CapacityBytes:    1000000,
		UsedBytes:        50000,
		ChunkCount:       5,
		RegisteredAt:     now,
		LastStatusChange: now,
	}

	if err := store.UpsertWorker(ctx, w); err != nil {
		t.Fatalf("UpsertWorker: %v", err)
	}

	workers, err := store.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 1 || workers[0].WorkerID != "worker1" {
		t.Fatalf("unexpected workers: %+v", workers)
	}

	// Status transition
	if err := store.SetWorkerStatus(ctx, "worker1", "SUSPECT", now+1000); err != nil {
		t.Fatalf("SetWorkerStatus: %v", err)
	}

	workers, _ = store.ListWorkers(ctx)
	if workers[0].Status != "SUSPECT" {
		t.Errorf("expected status SUSPECT, got %s", workers[0].Status)
	}

	// Admin counts test
	counts, err := store.ObjectCounts(ctx)
	if err != nil {
		t.Fatalf("ObjectCounts: %v", err)
	}
	if counts.Committed != 0 {
		t.Errorf("expected 0 committed objects, got %d", counts.Committed)
	}

	chCounts, err := store.ChunkCounts(ctx)
	if err != nil {
		t.Fatalf("ChunkCounts: %v", err)
	}
	if chCounts.Total != 0 || chCounts.CorruptReplicas != 0 {
		t.Errorf("unexpected chunk counts: %+v", chCounts)
	}
}
