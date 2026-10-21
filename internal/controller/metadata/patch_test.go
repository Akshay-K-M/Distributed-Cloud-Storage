package metadata_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"objstore/internal/controller/metadata"
)

func strPtr(s string) *string {
	return &s
}

func TestStore_PatchMetadata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "patch_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open failed: %v", err)
	}
	defer store.Close()

	id, err := store.BeginUpload(ctx, "patch.txt", "text/plain", false)
	if err != nil {
		t.Fatal(err)
	}
	initialObj, err := store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       id,
		Name:           "patch.txt",
		Size:           10,
		ChecksumSHA256: "h",
		ChunkCount:     1,
		CustomMetadata: map[string]string{
			"env":    "prod",
			"author": "bob",
		},
		Chunks: []metadata.ChunkCommit{
			{ChunkID: id + "_0", Index: 0, Size: 10, SHA256: "h", Replicas: []string{"w1"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Millisecond) // Ensure modified_at timestamp can advance

	// 1. Add key and update key and delete key
	patch := map[string]*string{
		"author":  strPtr("alice"),
		"version": strPtr("v1.2"),
		"env":     nil, // delete
	}

	updated, err := store.PatchMetadata(ctx, id, patch)
	if err != nil {
		t.Fatalf("PatchMetadata failed: %v", err)
	}

	if updated.CustomMetadata["author"] != "alice" {
		t.Errorf("expected author=alice, got %s", updated.CustomMetadata["author"])
	}
	if updated.CustomMetadata["version"] != "v1.2" {
		t.Errorf("expected version=v1.2, got %s", updated.CustomMetadata["version"])
	}
	if _, ok := updated.CustomMetadata["env"]; ok {
		t.Errorf("expected env key to be deleted")
	}
	if updated.ModifiedAt <= initialObj.ModifiedAt {
		t.Errorf("expected ModifiedAt to be updated: initial=%s, updated=%s", initialObj.ModifiedAt, updated.ModifiedAt)
	}

	// 2. Invalid key with spaces
	_, err = store.PatchMetadata(ctx, id, map[string]*string{
		"bad key": strPtr("val"),
	})
	if err != metadata.ErrInvalidRequest {
		t.Errorf("expected ErrInvalidRequest for bad key, got: %v", err)
	}

	// 3. Key too long (> 128 bytes)
	longKey := strings.Repeat("k", 129)
	_, err = store.PatchMetadata(ctx, id, map[string]*string{
		longKey: strPtr("val"),
	})
	if err != metadata.ErrInvalidRequest {
		t.Errorf("expected ErrInvalidRequest for long key, got: %v", err)
	}

	// 4. Value too long (> 1024 bytes)
	longVal := strings.Repeat("v", 1025)
	_, err = store.PatchMetadata(ctx, id, map[string]*string{
		"longval": strPtr(longVal),
	})
	if err != metadata.ErrInvalidRequest {
		t.Errorf("expected ErrInvalidRequest for long val, got: %v", err)
	}

	// 5. Total keys limit (> 64)
	tooMany := make(map[string]*string)
	for i := 0; i < 70; i++ {
		key := strings.Repeat("a", 5) + string(rune('a'+(i%26))) + string(rune('0'+(i/26)))
		tooMany[key] = strPtr("val")
	}
	_, err = store.PatchMetadata(ctx, id, tooMany)
	if err != metadata.ErrInvalidRequest {
		t.Errorf("expected ErrInvalidRequest for >64 keys, got: %v", err)
	}

	// 6. 404 on missing object
	_, err = store.PatchMetadata(ctx, "nonexistent-id", map[string]*string{
		"key": strPtr("val"),
	})
	if err != metadata.ErrNotFound {
		t.Errorf("expected ErrNotFound for missing object, got: %v", err)
	}
}
