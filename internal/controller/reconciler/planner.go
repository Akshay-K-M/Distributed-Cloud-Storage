// This file computes desired replica repairs, trims, and lost-chunk status.
package reconciler

import (
	"sort"
	"time"
)

// PlannerConfig holds policy knobs for the reconciler planner.
type PlannerConfig struct {
	ReplicationFactor int
	RepairDelay       time.Duration
	RepairEnabled     bool
}

// ReplicaState is the state of a single replica on a worker.
type ReplicaState struct {
	WorkerID string
	State    string // "OK" or "CORRUPT"
}

// ChunkState describes a committed chunk and all its known replicas.
type ChunkState struct {
	ChunkID  string
	Size     int64
	SHA      string
	Replicas []ReplicaState
}

// WorkerState captures a worker's status and last transition time.
type WorkerState struct {
	ID          string
	Status      string // "ALIVE", "SUSPECT", "DEAD"
	StatusSince time.Time
	UsedBytes   int64
}

// Action represents a reconciliation action.
type Action interface {
	actionType() string
}

// RepairChunk copies an under-replicated chunk from an OK replica to an alternate worker.
type RepairChunk struct {
	Chunk    ChunkState
	Sources  []string        // Worker IDs that have an OK replica
	Exclude  map[string]bool // Worker IDs that already hold any replica row
	Priority int             // 1 = highest (counted == 1), 2 = counted == 0 with DEAD source, 3 = other
}

func (RepairChunk) actionType() string { return "repair" }

// TrimReplica removes an excess replica from an over-replicated chunk.
type TrimReplica struct {
	ChunkID  string
	WorkerID string
}

func (TrimReplica) actionType() string { return "trim" }

// DeleteCorrupt deletes a verified corrupt replica once the chunk is sufficiently replicated.
type DeleteCorrupt struct {
	ChunkID  string
	WorkerID string
}

func (DeleteCorrupt) actionType() string { return "delete_corrupt" }

// PlanResult contains the planned actions and chunk health summary.
type PlanResult struct {
	Actions         []Action
	TotalChunks     int
	UnderReplicated int
	OverReplicated  int
	CorruptReplicas int
	Lost            int
}

// Plan produces the list of reconciliation actions for the cluster.
// It is a pure function without I/O.
func Plan(now time.Time, cfg PlannerConfig, chunks []ChunkState, workers map[string]WorkerState) PlanResult {
	res := PlanResult{
		TotalChunks: len(chunks),
	}

	var repairs []RepairChunk
	var trims []TrimReplica
	var deletes []DeleteCorrupt

	for _, ch := range chunks {
		counted := 0
		var okSources []string
		var aliveSources []string
		var deadOKSources []string
		var corruptReplicas []string
		exclude := make(map[string]bool)

		for _, rep := range ch.Replicas {
			exclude[rep.WorkerID] = true

			w, known := workers[rep.WorkerID]
			if !known {
				continue
			}

			if rep.State == "CORRUPT" {
				res.CorruptReplicas++
				corruptReplicas = append(corruptReplicas, rep.WorkerID)
				continue
			}

			if rep.State == "OK" {
				if w.Status == "ALIVE" {
					aliveSources = append(aliveSources, rep.WorkerID)
				}
				if w.Status == "ALIVE" || w.Status == "SUSPECT" {
					counted++
					okSources = append(okSources, rep.WorkerID)
				} else if w.Status == "DEAD" {
					elapsed := now.Sub(w.StatusSince)
					if elapsed < cfg.RepairDelay {
						// Within grace period: count it to avoid copy storms on blips
						counted++
						okSources = append(okSources, rep.WorkerID)
					} else {
						// DEAD past RepairDelay: does not count toward RF
						deadOKSources = append(deadOKSources, rep.WorkerID)
					}
				}
			}
		}

		if counted < cfg.ReplicationFactor {
			if len(okSources) == 0 && len(deadOKSources) == 0 {
				res.Lost++
			} else {
				res.UnderReplicated++
				if cfg.RepairEnabled {
					priority := 3
					if counted == 1 {
						priority = 1 // Highest priority: one failure away from data loss
					} else if counted == 0 && len(deadOKSources) > 0 {
						priority = 2
					}

					sources := okSources
					if len(sources) == 0 {
						sources = deadOKSources
					}

					repairs = append(repairs, RepairChunk{
						Chunk:    ch,
						Sources:  sources,
						Exclude:  exclude,
						Priority: priority,
					})
				}
			}
		} else if counted > cfg.ReplicationFactor {
			res.OverReplicated++
			if cfg.RepairEnabled {
				// Excess replicas to trim from most-used workers
				// A suspect or recently dead worker can delay a repair, but
				// its copy cannot justify deleting a reachable one.
				excess := len(aliveSources) - cfg.ReplicationFactor

				type candidate struct {
					workerID  string
					usedBytes int64
				}
				var candidates []candidate
				for _, wID := range aliveSources {
					candidates = append(candidates, candidate{
						workerID:  wID,
						usedBytes: workers[wID].UsedBytes,
					})
				}

				// Sort descending by UsedBytes
				sort.Slice(candidates, func(i, j int) bool {
					return candidates[i].usedBytes > candidates[j].usedBytes
				})

				for i := 0; i < excess && i < len(candidates); i++ {
					trims = append(trims, TrimReplica{
						ChunkID:  ch.ChunkID,
						WorkerID: candidates[i].workerID,
					})
				}
			}
		}

		// Delete corrupt copies if chunk already has enough OK copies
		if cfg.RepairEnabled && len(aliveSources) >= cfg.ReplicationFactor {
			for _, wID := range corruptReplicas {
				deletes = append(deletes, DeleteCorrupt{
					ChunkID:  ch.ChunkID,
					WorkerID: wID,
				})
			}
		}
	}

	// Sort repairs by Priority: 1 first, then 2, then 3
	sort.SliceStable(repairs, func(i, j int) bool {
		return repairs[i].Priority < repairs[j].Priority
	})

	for _, r := range repairs {
		res.Actions = append(res.Actions, r)
	}
	for _, t := range trims {
		res.Actions = append(res.Actions, t)
	}
	for _, d := range deletes {
		res.Actions = append(res.Actions, d)
	}

	return res
}
