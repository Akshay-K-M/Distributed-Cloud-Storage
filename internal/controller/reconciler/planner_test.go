// These tests verify desired-state planning for healthy and failed replicas.
package reconciler_test

import (
	"testing"
	"time"

	"objstore/internal/controller/reconciler"
)

func TestPlanner(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	cfg := reconciler.PlannerConfig{
		ReplicationFactor: 2,
		RepairDelay:       5 * time.Second,
		RepairEnabled:     true,
	}

	workers := map[string]reconciler.WorkerState{
		"w1": {ID: "w1", Status: "ALIVE", StatusSince: now, UsedBytes: 1000},
		"w2": {ID: "w2", Status: "ALIVE", StatusSince: now, UsedBytes: 2000},
		"w3": {ID: "w3", Status: "ALIVE", StatusSince: now, UsedBytes: 3000},
		"w4": {ID: "w4", Status: "ALIVE", StatusSince: now, UsedBytes: 4000},
	}

	t.Run("Healthy", func(t *testing.T) {
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"},
					{WorkerID: "w2", State: "OK"},
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, workers)
		if len(res.Actions) != 0 {
			t.Fatalf("expected 0 actions for healthy chunk, got %d", len(res.Actions))
		}
		if res.UnderReplicated != 0 || res.OverReplicated != 0 || res.Lost != 0 {
			t.Errorf("unexpected metrics: %+v", res)
		}
	})

	t.Run("One worker DEAD within delay -> no action", func(t *testing.T) {
		w := map[string]reconciler.WorkerState{
			"w1": {ID: "w1", Status: "ALIVE", StatusSince: now, UsedBytes: 1000},
			"w2": {ID: "w2", Status: "DEAD", StatusSince: now.Add(-3 * time.Second), UsedBytes: 2000}, // 3s < 5s delay
		}
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"},
					{WorkerID: "w2", State: "OK"},
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, w)
		if len(res.Actions) != 0 {
			t.Fatalf("expected 0 actions within grace period, got %d", len(res.Actions))
		}
		if res.UnderReplicated != 0 {
			t.Errorf("expected 0 under-replicated within delay, got %d", res.UnderReplicated)
		}
	})

	t.Run("One worker DEAD past delay -> RepairChunk", func(t *testing.T) {
		w := map[string]reconciler.WorkerState{
			"w1": {ID: "w1", Status: "ALIVE", StatusSince: now, UsedBytes: 1000},
			"w2": {ID: "w2", Status: "DEAD", StatusSince: now.Add(-10 * time.Second), UsedBytes: 2000}, // 10s > 5s delay
		}
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"},
					{WorkerID: "w2", State: "OK"},
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, w)
		if len(res.Actions) != 1 {
			t.Fatalf("expected 1 action, got %d", len(res.Actions))
		}
		repair, ok := res.Actions[0].(reconciler.RepairChunk)
		if !ok {
			t.Fatalf("expected RepairChunk action, got %T", res.Actions[0])
		}
		if repair.Chunk.ChunkID != "c1" || len(repair.Sources) != 1 || repair.Sources[0] != "w1" {
			t.Errorf("unexpected repair chunk details: %+v", repair)
		}
		if repair.Priority != 1 {
			t.Errorf("expected Priority 1 for counted==1, got %d", repair.Priority)
		}
		if res.UnderReplicated != 1 {
			t.Errorf("expected UnderReplicated 1, got %d", res.UnderReplicated)
		}
	})

	t.Run("Counted == 1 prioritized over counted == 0 with DEAD worker", func(t *testing.T) {
		w := map[string]reconciler.WorkerState{
			"w1": {ID: "w1", Status: "ALIVE", StatusSince: now, UsedBytes: 1000},
			"w2": {ID: "w2", Status: "DEAD", StatusSince: now.Add(-10 * time.Second), UsedBytes: 2000},
		}
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c_counted_0",
				Size:    100,
				SHA:     "sha0",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w2", State: "OK"}, // only on dead worker
				},
			},
			{
				ChunkID: "c_counted_1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"}, // on alive worker
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, w)
		if len(res.Actions) != 2 {
			t.Fatalf("expected 2 repair actions, got %d", len(res.Actions))
		}
		r0 := res.Actions[0].(reconciler.RepairChunk)
		r1 := res.Actions[1].(reconciler.RepairChunk)
		if r0.Chunk.ChunkID != "c_counted_1" || r1.Chunk.ChunkID != "c_counted_0" {
			t.Errorf("expected c_counted_1 first, got %s then %s", r0.Chunk.ChunkID, r1.Chunk.ChunkID)
		}
	})

	t.Run("a suspect copy cannot justify dropping a live copy", func(t *testing.T) {
		w := map[string]reconciler.WorkerState{
			"w1": {Status: "ALIVE"}, "w2": {Status: "ALIVE"}, "w3": {Status: "SUSPECT"},
		}
		chunk := reconciler.ChunkState{ChunkID: "c", Replicas: []reconciler.ReplicaState{
			{WorkerID: "w1", State: "OK"}, {WorkerID: "w2", State: "OK"}, {WorkerID: "w3", State: "OK"},
		}}
		if result := reconciler.Plan(now, cfg, []reconciler.ChunkState{chunk}, w); len(result.Actions) != 0 {
			t.Fatalf("premature trim: %+v", result.Actions)
		}
	})

	t.Run("Over-replication trimming picks most-used worker", func(t *testing.T) {
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"}, // used 1000
					{WorkerID: "w2", State: "OK"}, // used 2000
					{WorkerID: "w3", State: "OK"}, // used 3000 -> most used, should be trimmed
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, workers)
		if len(res.Actions) != 1 {
			t.Fatalf("expected 1 trim action, got %d", len(res.Actions))
		}
		trim, ok := res.Actions[0].(reconciler.TrimReplica)
		if !ok {
			t.Fatalf("expected TrimReplica, got %T", res.Actions[0])
		}
		if trim.WorkerID != "w3" {
			t.Errorf("expected trim to pick most-used worker w3, got %s", trim.WorkerID)
		}
		if res.OverReplicated != 1 {
			t.Errorf("expected OverReplicated 1, got %d", res.OverReplicated)
		}
	})

	t.Run("Corrupt replica deleted when RF is satisfied", func(t *testing.T) {
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"},
					{WorkerID: "w2", State: "OK"},
					{WorkerID: "w3", State: "CORRUPT"},
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, workers)
		if len(res.Actions) != 1 {
			t.Fatalf("expected 1 delete corrupt action, got %d", len(res.Actions))
		}
		del, ok := res.Actions[0].(reconciler.DeleteCorrupt)
		if !ok {
			t.Fatalf("expected DeleteCorrupt, got %T", res.Actions[0])
		}
		if del.WorkerID != "w3" {
			t.Errorf("expected delete corrupt on w3, got %s", del.WorkerID)
		}
		if res.CorruptReplicas != 1 {
			t.Errorf("expected CorruptReplicas 1, got %d", res.CorruptReplicas)
		}
	})

	t.Run("Lost chunk reported and not looped into repair", func(t *testing.T) {
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "lost_chunk",
				Size:    100,
				SHA:     "shalost",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "CORRUPT"},
					{WorkerID: "w2", State: "CORRUPT"},
				},
			},
		}
		res := reconciler.Plan(now, cfg, chunks, workers)
		if len(res.Actions) != 0 {
			t.Fatalf("expected 0 repair actions for lost chunk, got %d", len(res.Actions))
		}
		if res.Lost != 1 {
			t.Errorf("expected Lost count 1, got %d", res.Lost)
		}
	})

	t.Run("RepairEnabled=false yields no repair or trim actions but status reports under-replicated", func(t *testing.T) {
		noRepairCfg := cfg
		noRepairCfg.RepairEnabled = false

		w := map[string]reconciler.WorkerState{
			"w1": {ID: "w1", Status: "ALIVE", StatusSince: now, UsedBytes: 1000},
			"w2": {ID: "w2", Status: "DEAD", StatusSince: now.Add(-10 * time.Second), UsedBytes: 2000},
		}
		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"},
					{WorkerID: "w2", State: "OK"},
				},
			},
		}
		res := reconciler.Plan(now, noRepairCfg, chunks, w)
		if len(res.Actions) != 0 {
			t.Fatalf("expected 0 actions with RepairEnabled=false, got %d", len(res.Actions))
		}
		if res.UnderReplicated != 1 {
			t.Errorf("status must still report UnderReplicated=1, got %d", res.UnderReplicated)
		}
	})

	t.Run("RF=3 and W=2 yields async repair of third copy", func(t *testing.T) {
		rf3Cfg := cfg
		rf3Cfg.ReplicationFactor = 3

		chunks := []reconciler.ChunkState{
			{
				ChunkID: "c1",
				Size:    100,
				SHA:     "sha1",
				Replicas: []reconciler.ReplicaState{
					{WorkerID: "w1", State: "OK"},
					{WorkerID: "w2", State: "OK"},
				},
			},
		}
		res := reconciler.Plan(now, rf3Cfg, chunks, workers)
		if len(res.Actions) != 1 {
			t.Fatalf("expected 1 repair action for 3rd copy, got %d", len(res.Actions))
		}
		repair := res.Actions[0].(reconciler.RepairChunk)
		if repair.Priority != 3 {
			t.Errorf("expected priority 3 for counted=2 < RF=3, got %d", repair.Priority)
		}
	})
}
