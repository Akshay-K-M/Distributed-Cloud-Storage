// These tests verify worker registration, heartbeat recovery, and liveness transitions.
package registry_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/registry"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestRegistry_LivenessTransitionsAndRecovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "reg_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: start}

	reg := registry.New(store, 6*time.Second, 15*time.Second, clock, nil)

	var transitions []string
	reg.OnStatusChange(func(id, from, to string) {
		transitions = append(transitions, id+":"+from+"->"+to)
	})

	// 1. Register worker1
	err = reg.Register(ctx, api.RegisterRequest{
		WorkerID:      "worker1",
		Address:       "http://worker1:9000",
		CapacityBytes: 1000000,
		UsedBytes:     1000,
		ChunkCount:    1,
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	info, ok := reg.Get("worker1")
	if !ok || info.Status != "ALIVE" {
		t.Fatalf("expected worker1 to be ALIVE, got: %+v", info)
	}

	// 2. Advance clock by 5s (< 6s suspect threshold) and sweep
	clock.Advance(5 * time.Second)
	reg.Sweep(ctx)
	info, _ = reg.Get("worker1")
	if info.Status != "ALIVE" {
		t.Errorf("expected ALIVE at 5s, got %s", info.Status)
	}

	// 3. Advance clock past 6s -> should become SUSPECT
	clock.Advance(2 * time.Second) // total 7s
	reg.Sweep(ctx)
	info, _ = reg.Get("worker1")
	if info.Status != "SUSPECT" {
		t.Errorf("expected SUSPECT at 7s, got %s", info.Status)
	}

	// Verify DB status
	workers, _ := store.ListWorkers(ctx)
	if len(workers) != 1 || workers[0].Status != "SUSPECT" {
		t.Errorf("expected DB status SUSPECT, got %+v", workers)
	}

	// 4. Advance clock past 15s -> should become DEAD
	clock.Advance(9 * time.Second) // total 16s
	reg.Sweep(ctx)
	info, _ = reg.Get("worker1")
	if info.Status != "DEAD" {
		t.Errorf("expected DEAD at 16s, got %s", info.Status)
	}

	workers, _ = store.ListWorkers(ctx)
	if workers[0].Status != "DEAD" {
		t.Errorf("expected DB status DEAD, got %s", workers[0].Status)
	}

	// 5. Worker heartbeats back -> recovers to ALIVE
	clock.Advance(1 * time.Second)
	err = reg.Heartbeat(ctx, api.HeartbeatRequest{
		WorkerID:      "worker1",
		UsedBytes:     1050,
		CapacityBytes: 1000000,
		ChunkCount:    1,
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	info, _ = reg.Get("worker1")
	if info.Status != "ALIVE" {
		t.Errorf("expected worker to recover to ALIVE on heartbeat, got %s", info.Status)
	}

	// 6. Unknown worker heartbeat returns ErrUnknownWorker
	err = reg.Heartbeat(ctx, api.HeartbeatRequest{
		WorkerID: "worker99",
	})
	if err != registry.ErrUnknownWorker {
		t.Errorf("expected ErrUnknownWorker, got: %v", err)
	}

	// Verify transitions
	expectedTransitions := []string{
		"worker1:->ALIVE",
		"worker1:ALIVE->SUSPECT",
		"worker1:SUSPECT->DEAD",
		"worker1:DEAD->ALIVE",
	}
	if len(transitions) != len(expectedTransitions) {
		t.Errorf("expected %d transitions, got %v", len(expectedTransitions), transitions)
	}
}

func TestRegistry_ReadCandidates(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "candidates.db")
	store, _ := metadata.Open(dbPath)
	defer store.Close()

	clock := &fakeClock{now: time.Now()}
	reg := registry.New(store, 6*time.Second, 15*time.Second, clock, nil)

	// w1: ALIVE, InFlight 2
	// w2: ALIVE, InFlight 0
	// w3: SUSPECT
	// w4: DEAD
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w1", Address: "http://w1:9000"})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w2", Address: "http://w2:9000"})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w3", Address: "http://w3:9000"})
	_ = reg.Register(ctx, api.RegisterRequest{WorkerID: "w4", Address: "http://w4:9000"})

	reg.IncInFlight("w1")
	reg.IncInFlight("w1")

	clock.Advance(7 * time.Second)
	// sweep makes all 4 SUSPECT
	reg.Sweep(ctx)
	// recover w1 and w2 to ALIVE
	_ = reg.Heartbeat(ctx, api.HeartbeatRequest{WorkerID: "w1"})
	_ = reg.Heartbeat(ctx, api.HeartbeatRequest{WorkerID: "w2"})
	// advance clock 10s more so w4 becomes DEAD (w3 remains SUSPECT)
	clock.Advance(10 * time.Second)
	// w3 is at 17s total -> DEAD. Let's make sure w3 is SUSPECT by sending heartbeat at 12s
	_ = reg.Heartbeat(ctx, api.HeartbeatRequest{WorkerID: "w3"}) // w3 alive at 17s
	clock.Advance(7 * time.Second)                               // w3 at 7s -> SUSPECT; w4 at 24s -> DEAD
	reg.Sweep(ctx)
	_ = reg.Heartbeat(ctx, api.HeartbeatRequest{WorkerID: "w1"})
	_ = reg.Heartbeat(ctx, api.HeartbeatRequest{WorkerID: "w2"})

	candidates := reg.ReadCandidates([]string{"w4", "w3", "w1", "w2"})
	// w4 is DEAD -> excluded
	// w2 (ALIVE, inFlight 0) must be first
	// w1 (ALIVE, inFlight 2) must be second
	// w3 (SUSPECT) must be third
	if len(candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %v", candidates)
	}
	if candidates[0] != "w2" || candidates[1] != "w1" || candidates[2] != "w3" {
		t.Errorf("unexpected candidates ordering: %v", candidates)
	}
}

func TestRegistry_ControllerRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "restart.db")
	store, _ := metadata.Open(dbPath)
	defer store.Close()

	clock := &fakeClock{now: time.Now()}
	reg1 := registry.New(store, 6*time.Second, 15*time.Second, clock, nil)
	_ = reg1.Register(ctx, api.RegisterRequest{WorkerID: "w1", Address: "http://w1:9000"})
	_ = reg1.Register(ctx, api.RegisterRequest{WorkerID: "w2", Address: "http://w2:9000"})

	// Simulate controller restart with fresh in-memory registry loading from DB
	reg2 := registry.New(store, 6*time.Second, 15*time.Second, clock, nil)
	if err := reg2.LoadFromDB(ctx); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}

	// Should be loaded as SUSPECT
	info, ok := reg2.Get("w1")
	if !ok || info.Status != "SUSPECT" {
		t.Errorf("expected w1 to be loaded as SUSPECT, got %+v", info)
	}

	// Heartbeat moves it to ALIVE
	_ = reg2.Heartbeat(ctx, api.HeartbeatRequest{WorkerID: "w1"})
	info, _ = reg2.Get("w1")
	if info.Status != "ALIVE" {
		t.Errorf("expected w1 to become ALIVE, got %s", info.Status)
	}
}
