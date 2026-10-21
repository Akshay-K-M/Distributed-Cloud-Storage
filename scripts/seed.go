//go:build dev

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"objstore/internal/common/config"
	"objstore/internal/controller/metadata"
)

func main() {
	dbPath := config.Env("DB_PATH", "./meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		log.Fatalf("failed to open metadata store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	fixtures := []struct {
		name string
		size int64
		ct   string
		meta map[string]string
	}{
		{"photos/cat.jpg", 3145729, "image/jpeg", map[string]string{"owner": "alice", "tag": "animal"}},
		{"photos/dog.jpg", 1048576, "image/jpeg", map[string]string{"owner": "bob"}},
		{"docs/report.pdf", 204800, "application/pdf", map[string]string{"dept": "finance"}},
		{"README.txt", 1234, "text/plain", map[string]string{}},
	}

	for i, f := range fixtures {
		id, err := store.BeginUpload(ctx, f.name, f.ct, true)
		if err != nil {
			log.Fatalf("begin upload %s: %v", f.name, err)
		}

		chunkCount := int((f.size + 1048575) / 1048576)
		if chunkCount == 0 {
			chunkCount = 1
		}

		chunks := make([]metadata.ChunkCommit, chunkCount)
		for c := 0; c < chunkCount; c++ {
			chunks[c] = metadata.ChunkCommit{
				ChunkID:  fmt.Sprintf("%s_%d", id, c),
				Index:    c,
				Size:     1048576,
				SHA256:   fmt.Sprintf("mocksha256chunk%d", c),
				Replicas: []string{"worker1", "worker2"},
			}
		}

		obj, err := store.CommitObject(ctx, metadata.CommitInput{
			ObjectID:       id,
			Name:           f.name,
			Size:           f.size,
			ChecksumSHA256: fmt.Sprintf("mockobjchecksum%d", i),
			ChunkCount:     chunkCount,
			ContentType:    f.ct,
			CustomMetadata: f.meta,
			Overwrite:      true,
			Chunks:         chunks,
		})
		if err != nil {
			log.Fatalf("commit object %s: %v", f.name, err)
		}
		fmt.Printf("Seeded object: %s (id: %s)\n", obj.Name, obj.ObjectID)
	}

	fmt.Println("Seeding complete.")
	_ = os.Stdout
}
