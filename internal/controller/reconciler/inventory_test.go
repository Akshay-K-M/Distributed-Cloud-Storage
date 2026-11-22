// These tests verify worker inventory reconciliation and replica state updates.
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

type mockInFlight struct {
	inFlight map[string]bool
}

func (m *mockInFlight) IsInFlight(id string) bool {
	return m.inFlight[id]
}

func TestInventorySync(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "inv_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	reg := registry.New(store, 6*time.Second, 15*time.Second, nil, nil)
	fakeClient := workerclient.NewFake()
	inFlight := &mockInFlight{inFlight: make(map[string]bool)}

	wRef := workerclient.WorkerRef{ID: "w1", Addr: "http://w1:9000"}
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w1", Address: "http://w1:9000"})

	woken := false
	syncer := reconciler.NewInventorySyncer(store, reg, fakeClient, inFlight, func() {
		woken = true
	}, nil)

	data := []byte("hello inventory")
	sha := checksum.SHA256Hex(data)

	// 1. Seed a committed object
	objID, _ := store.BeginUpload(ctx, "file.txt", "text/plain", false)
	chunkID := objID + "_0"
	_, _ = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       objID,
		Name:           "file.txt",
		Size:           int64(len(data)),
		ChecksumSHA256: sha,
		ChunkCount:     1,
		Chunks: []metadata.ChunkCommit{
			{ChunkID: chunkID, Index: 0, Size: int64(len(data)), SHA256: sha, Replicas: []string{"w1"}},
		},
	})

	// Put chunk on w1
	_ = fakeClient.PutChunk(ctx, wRef, chunkID, sha, int64(len(data)), bytes.NewReader(data))

	// Put an orphan chunk on w1 (not in DB)
	orphanID := "orphan_chunk_0"
	_ = fakeClient.PutChunk(ctx, wRef, orphanID, sha, int64(len(data)), bytes.NewReader(data))

	// Put an in-flight upload chunk on w1
	inFlightObjID := "inflight_obj_id"
	inFlightChunkID := inFlightObjID + "_0"
	inFlight.inFlight[inFlightObjID] = true
	_ = fakeClient.PutChunk(ctx, wRef, inFlightChunkID, sha, int64(len(data)), bytes.NewReader(data))

	// Run Inventory Sync
	if err := syncer.SyncWorker(ctx, "w1"); err != nil {
		t.Fatalf("SyncWorker failed: %v", err)
	}

	// Assertions:
	// - Orphan chunk should be deleted
	if _, _, _, err := fakeClient.GetChunk(ctx, wRef, orphanID); err != workerclient.ErrChunkNotFound {
		t.Errorf("expected orphan chunk to be deleted, got err: %v", err)
	}

	// - In-flight chunk should NOT be deleted
	if _, _, _, err := fakeClient.GetChunk(ctx, wRef, inFlightChunkID); err != nil {
		t.Errorf("in-flight chunk should not be deleted: %v", err)
	}

	// - Committed chunk should remain
	if _, _, _, err := fakeClient.GetChunk(ctx, wRef, chunkID); err != nil {
		t.Errorf("committed chunk should remain: %v", err)
	}

	// - Reconciler wake hook should have been called
	if !woken {
		t.Errorf("expected reconciler to be woken after sync")
	}

	// A matching sidecar cannot overrule corruption found by a verified read.
	if err := store.SetReplicaState(ctx, chunkID, "w1", "CORRUPT", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncWorker(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	checked, err := store.GetObjectWithChunks(ctx, objID)
	if err != nil || checked.Chunks[0].Replicas[0].State != "CORRUPT" {
		t.Fatalf("inventory cleared a corruption finding: %+v, %v", checked, err)
	}

	// A quarantined copy remains associated with its object until cleanup.
	fakeClient.Quarantine("w1", chunkID)
	fakeClient.Quarantine("w1", orphanID)
	if err := syncer.SyncWorker(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	damaged, err := store.GetObjectWithChunks(ctx, objID)
	if err != nil {
		t.Fatal(err)
	}
	replicas := damaged.Chunks[0].Replicas
	if len(replicas) != 1 || replicas[0].State != "CORRUPT" {
		t.Fatalf("expected a recorded corrupt replica: %+v", replicas)
	}
	inv, err := fakeClient.ListChunks(ctx, wRef)
	if err != nil || len(inv.Quarantined) != 1 || inv.Quarantined[0] != chunkID {
		t.Fatalf("quarantined orphan cleanup: %+v, %v", inv, err)
	}

	// 2. Test lost chunk: delete chunk physically from worker, sync should remove DB replica row
	_ = fakeClient.DeleteChunk(ctx, wRef, chunkID)
	_ = syncer.SyncWorker(ctx, "w1")

	detail, _ := store.GetObjectWithChunks(ctx, objID)
	if len(detail.Chunks[0].Replicas) != 0 {
		t.Errorf("expected replica row to be deleted in DB after worker lost chunk, got %d", len(detail.Chunks[0].Replicas))
	}
}
