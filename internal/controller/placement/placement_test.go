// These tests verify deterministic worker placement and capacity balancing.
package placement_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
)

func TestPlacement_PickAndRelease(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "placer_test.db")
	store, _ := metadata.Open(dbPath)
	defer store.Close()

	reg := registry.New(store, 6*time.Second, 15*time.Second, nil, nil)
	placer := placement.New(reg)

	// Register 4 workers:
	// w1: used 1000
	// w2: used 2000
	// w3: used 500
	// w4: used 4000, capacity 5000
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w1", Address: "http://w1:9000", UsedBytes: 1000, CapacityBytes: 10000})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w2", Address: "http://w2:9000", UsedBytes: 2000, CapacityBytes: 10000})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w3", Address: "http://w3:9000", UsedBytes: 500, CapacityBytes: 10000})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w4", Address: "http://w4:9000", UsedBytes: 4000, CapacityBytes: 5000})

	// 1. Pick 2 workers: should pick least-loaded (w3 with 500, w1 with 1000)
	picked, err := placer.Pick(2, nil, 100)
	if err != nil {
		t.Fatalf("Pick(2) failed: %v", err)
	}
	if len(picked) != 2 {
		t.Fatalf("expected 2 picked, got %d", len(picked))
	}
	if picked[0].ID != "w3" || picked[1].ID != "w1" {
		t.Errorf("expected w3 and w1, got %s and %s", picked[0].ID, picked[1].ID)
	}

	// 2. Optimistic accounting check: w3 should have +100 usedBytes, w1 should have +100
	infoW3, _ := reg.Get("w3")
	if infoW3.UsedBytes != 600 {
		t.Errorf("expected w3 to have 600 usedBytes after pick, got %d", infoW3.UsedBytes)
	}
	infoW1, _ := reg.Get("w1")
	if infoW1.UsedBytes != 1100 {
		t.Errorf("expected w1 to have 1100 usedBytes after pick, got %d", infoW1.UsedBytes)
	}

	// 3. Release w1
	placer.Release("w1", 100)
	infoW1, _ = reg.Get("w1")
	if infoW1.UsedBytes != 1000 {
		t.Errorf("expected w1 to be restored to 1000 usedBytes after release, got %d", infoW1.UsedBytes)
	}

	// 4. Exclude set
	pickedExclude, err := placer.Pick(2, map[string]bool{"w3": true}, 100)
	if err != nil {
		t.Fatalf("Pick with exclude failed: %v", err)
	}
	for _, p := range pickedExclude {
		if p.ID == "w3" {
			t.Errorf("w3 should have been excluded")
		}
	}

	// 5. Capacity filter: try picking for size 2000; w4 has remaining 5000-4000=1000 < 2000, so w4 cannot be chosen
	pickedCap, err := placer.Pick(3, nil, 2000)
	if err != nil {
		t.Fatalf("Pick 3 with size 2000 failed: %v", err)
	}
	for _, p := range pickedCap {
		if p.ID == "w4" {
			t.Errorf("w4 should have been filtered out due to capacity")
		}
	}

	// 6. Insufficient workers
	_, err = placer.Pick(10, nil, 100)
	if err != placement.ErrInsufficientWorkers {
		t.Errorf("expected ErrInsufficientWorkers, got: %v", err)
	}
}
