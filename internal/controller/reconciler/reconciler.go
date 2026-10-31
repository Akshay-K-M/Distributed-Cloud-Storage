// This file runs the level-triggered background reconciliation loop.
package reconciler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"objstore/internal/controller"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

// Reconciler coordinates the level-triggered repair loop, GC, and inventory sync.
type Reconciler struct {
	cfg       controller.Config
	store     metadata.Store
	registry  *registry.Registry
	placer    *placement.Placer
	client    workerclient.Client
	executor  *Executor
	gc        *GarbageCollector
	inventory *InventorySyncer
	log       *slog.Logger

	wakeCh    chan struct{}
	runningMu sync.Mutex
	startTime time.Time

	lastResultMu sync.RWMutex
	lastResult   PlanResult
}

// New creates a new Reconciler.
func New(
	cfg controller.Config,
	store metadata.Store,
	reg *registry.Registry,
	placer *placement.Placer,
	client workerclient.Client,
	inFlight InFlightChecker,
	log *slog.Logger,
) *Reconciler {
	if log == nil {
		log = slog.Default()
	}

	r := &Reconciler{
		cfg:       cfg,
		store:     store,
		registry:  reg,
		placer:    placer,
		client:    client,
		wakeCh:    make(chan struct{}, 1),
		startTime: time.Now(),
		log:       log,
	}

	r.executor = NewExecutor(store, reg, placer, client, cfg.RepairConcurrency, log)
	r.gc = NewGarbageCollector(store, reg, client, log)
	r.inventory = NewInventorySyncer(store, reg, client, inFlight, r.Wake, log)

	return r
}

// Wake signals the reconciler to trigger an immediate pass.
func (r *Reconciler) Wake() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

// InventorySyncer returns the associated InventorySyncer.
func (r *Reconciler) InventorySyncer() *InventorySyncer {
	return r.inventory
}

// SyncWorker triggers inventory synchronization for a single worker.
func (r *Reconciler) SyncWorker(ctx context.Context, workerID string) error {
	return r.inventory.SyncWorker(ctx, workerID)
}

// SyncAllWorkers triggers inventory synchronization for all alive workers.
func (r *Reconciler) SyncAllWorkers(ctx context.Context) {
	r.inventory.SyncAllAliveWorkers(ctx)
}

// Start begins the background reconciliation loops.
func (r *Reconciler) Start(ctx context.Context) {
	// 1. Reconcile loop (every RECONCILE_INTERVAL or on Wake)
	go func() {
		ticker := time.NewTicker(r.cfg.ReconcileInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.RunPass(ctx)
			case <-r.wakeCh:
				r.RunPass(ctx)
			}
		}
	}()

	// 2. Periodic inventory sync loop (every INVENTORY_SYNC_INTERVAL)
	go func() {
		ticker := time.NewTicker(r.cfg.InventorySyncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.inventory.SyncAllAliveWorkers(ctx)
			}
		}
	}()
}

// RunPass executes a single non-overlapping reconciliation pass.
func (r *Reconciler) RunPass(ctx context.Context) {
	if !r.runningMu.TryLock() {
		return // Never run overlapping passes
	}
	defer r.runningMu.Unlock()

	// Skip until STARTUP_GRACE after controller start
	if time.Since(r.startTime) < r.cfg.StartupGrace {
		return
	}

	// 1. Run Garbage Collection for DELETING objects
	if err := r.gc.Run(ctx); err != nil {
		r.log.Warn("reconciler gc pass error", "err", err)
	}

	// 2. Load cluster chunk and worker states
	chunks, workers, err := r.loadClusterState(ctx)
	if err != nil {
		r.log.Error("failed to load cluster state for reconciliation", "err", err)
		return
	}

	// 3. Plan actions
	planCfg := PlannerConfig{
		ReplicationFactor: r.cfg.ReplicationFactor,
		RepairDelay:       r.cfg.RepairDelay,
		RepairEnabled:     r.cfg.RepairEnabled,
	}

	planResult := Plan(time.Now(), planCfg, chunks, workers)

	r.lastResultMu.Lock()
	r.lastResult = planResult
	r.lastResultMu.Unlock()

	if len(planResult.Actions) == 0 {
		return
	}

	r.log.Info("reconciler planned actions", "actions", len(planResult.Actions),
		"under_replicated", planResult.UnderReplicated,
		"over_replicated", planResult.OverReplicated,
		"corrupt", planResult.CorruptReplicas)

	// 4. Execute actions
	if err := r.executor.Execute(ctx, planResult.Actions); err != nil {
		r.log.Warn("reconciler executor error", "err", err)
	}
}

// StatusCounts returns the latest chunk replica counts for admin status.
func (r *Reconciler) StatusCounts(ctx context.Context) (under, over, corrupt, lost int, err error) {
	chunks, workers, err := r.loadClusterState(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	planCfg := PlannerConfig{
		ReplicationFactor: r.cfg.ReplicationFactor,
		RepairDelay:       r.cfg.RepairDelay,
		RepairEnabled:     r.cfg.RepairEnabled,
	}

	res := Plan(time.Now(), planCfg, chunks, workers)
	return res.UnderReplicated, res.OverReplicated, res.CorruptReplicas, res.Lost, nil
}

func (r *Reconciler) loadClusterState(ctx context.Context) ([]ChunkState, map[string]WorkerState, error) {
	chunkInfos, err := r.store.ListChunksForReconciliation(ctx)
	if err != nil {
		return nil, nil, err
	}

	chunks := make([]ChunkState, len(chunkInfos))
	for i, ci := range chunkInfos {
		reps := make([]ReplicaState, len(ci.Replicas))
		for j, r := range ci.Replicas {
			reps[j] = ReplicaState{WorkerID: r.WorkerID, State: r.State}
		}
		chunks[i] = ChunkState{
			ChunkID:  ci.ChunkID,
			Size:     ci.Size,
			SHA:      ci.SHA256,
			Replicas: reps,
		}
	}

	workerRows, err := r.store.ListWorkers(ctx)
	if err != nil {
		return nil, nil, err
	}

	workers := make(map[string]WorkerState)
	for _, wr := range workerRows {
		workers[wr.WorkerID] = WorkerState{
			ID:          wr.WorkerID,
			Status:      wr.Status,
			StatusSince: time.UnixMilli(wr.LastStatusChange),
			UsedBytes:   wr.UsedBytes,
		}
	}

	return chunks, workers, nil
}
