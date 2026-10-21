package registry

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"objstore/internal/api"
	"objstore/internal/controller/metadata"
)

var ErrUnknownWorker = errors.New("unknown worker")

// Clock provides current time, allowing tests to mock time.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

// Worker represents an in-memory worker node state.
type Worker struct {
	ID         string
	Addr       string
	Status     string // ALIVE, SUSPECT, DEAD
	LastSeen   time.Time
	UsedBytes  int64
	Capacity   int64
	ChunkCount int
	InFlight   atomic.Int32
}

// WorkerInfo is an exported immutable snapshot of worker state.
type WorkerInfo struct {
	ID            string
	Addr          string
	Status        string
	LastSeen      time.Time
	UsedBytes     int64
	Capacity      int64
	ChunkCount    int
	InFlight      int32
	LastSeenMSAgo int64
}

// StatusChangeHook is called whenever a worker's status transitions.
type StatusChangeHook func(workerID, from, to string)

// Registry manages worker node liveness in-memory with DB persistence on transitions.
type Registry struct {
	mu           sync.RWMutex
	workers      map[string]*Worker
	store        metadata.Store
	clock        Clock
	log          *slog.Logger
	suspectAfter time.Duration
	deadAfter    time.Duration
	hooks        []StatusChangeHook
}

// New creates a new Registry.
func New(store metadata.Store, suspectAfter, deadAfter time.Duration, clock Clock, log *slog.Logger) *Registry {
	if clock == nil {
		clock = RealClock{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Registry{
		workers:      make(map[string]*Worker),
		store:        store,
		clock:        clock,
		log:          log,
		suspectAfter: suspectAfter,
		deadAfter:    deadAfter,
	}
}

// OnStatusChange registers a subscriber callback for status change events.
func (r *Registry) OnStatusChange(hook StatusChangeHook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks = append(r.hooks, hook)
}

func (r *Registry) notifyStatusChange(workerID, from, to string) {
	r.log.Info("worker status change", "worker_id", workerID, "from", from, "to", to)
	for _, h := range r.hooks {
		h(workerID, from, to)
	}
}

// LoadFromDB restores known workers from database and marks them SUSPECT on controller startup.
func (r *Registry) LoadFromDB(ctx context.Context) error {
	workers, err := r.store.ListWorkers(ctx)
	if err != nil {
		return err
	}

	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, w := range workers {
		r.workers[w.WorkerID] = &Worker{
			ID:         w.WorkerID,
			Addr:       w.Address,
			Status:     "SUSPECT", // Start as SUSPECT so they heartbeat back or become DEAD
			LastSeen:   now,
			UsedBytes:  w.UsedBytes,
			Capacity:   w.CapacityBytes,
			ChunkCount: w.ChunkCount,
		}
	}
	return nil
}

// Register adds or updates a worker as ALIVE and persists to database.
func (r *Registry) Register(ctx context.Context, req api.RegisterRequest) error {
	now := r.clock.Now()
	nowMs := now.UnixMilli()

	r.mu.Lock()
	w, exists := r.workers[req.WorkerID]
	oldStatus := ""
	if !exists {
		w = &Worker{
			ID: req.WorkerID,
		}
		r.workers[req.WorkerID] = w
	} else {
		oldStatus = w.Status
	}

	w.Addr = req.Address
	w.Status = "ALIVE"
	w.LastSeen = now
	w.Capacity = req.CapacityBytes
	w.UsedBytes = req.UsedBytes
	w.ChunkCount = req.ChunkCount

	row := metadata.WorkerRow{
		WorkerID:         w.ID,
		Address:          w.Addr,
		Status:           "ALIVE",
		CapacityBytes:    w.Capacity,
		UsedBytes:        w.UsedBytes,
		ChunkCount:       w.ChunkCount,
		RegisteredAt:     nowMs,
		LastStatusChange: nowMs,
	}
	r.mu.Unlock()

	// Persist to DB
	err := r.store.UpsertWorker(ctx, row)
	if err != nil {
		return err
	}

	if oldStatus != "ALIVE" {
		r.notifyStatusChange(req.WorkerID, oldStatus, "ALIVE")
	}

	return nil
}

// Heartbeat processes a worker heartbeat. Returns ErrUnknownWorker if not registered.
func (r *Registry) Heartbeat(ctx context.Context, req api.HeartbeatRequest) error {
	now := r.clock.Now()
	nowMs := now.UnixMilli()

	r.mu.Lock()
	w, ok := r.workers[req.WorkerID]
	if !ok {
		r.mu.Unlock()
		return ErrUnknownWorker
	}

	oldStatus := w.Status
	w.LastSeen = now
	w.UsedBytes = req.UsedBytes
	w.Capacity = req.CapacityBytes
	w.ChunkCount = req.ChunkCount
	statusChanged := false

	if oldStatus == "SUSPECT" || oldStatus == "DEAD" {
		w.Status = "ALIVE"
		statusChanged = true
	}
	r.mu.Unlock()

	if statusChanged {
		if err := r.store.SetWorkerStatus(ctx, req.WorkerID, "ALIVE", nowMs); err != nil {
			r.log.Error("failed to persist worker status recovery", "worker_id", req.WorkerID, "err", err)
		}
		r.notifyStatusChange(req.WorkerID, oldStatus, "ALIVE")
	}

	return nil
}

// Sweep checks worker last seen timestamps and executes status transitions.
func (r *Registry) Sweep(ctx context.Context) {
	now := r.clock.Now()
	nowMs := now.UnixMilli()

	type transition struct {
		id   string
		from string
		to   string
	}
	var transitions []transition

	r.mu.Lock()
	for _, w := range r.workers {
		elapsed := now.Sub(w.LastSeen)
		if w.Status == "ALIVE" && elapsed >= r.suspectAfter {
			transitions = append(transitions, transition{id: w.ID, from: "ALIVE", to: "SUSPECT"})
			w.Status = "SUSPECT"
		} else if w.Status == "SUSPECT" && elapsed >= r.deadAfter {
			transitions = append(transitions, transition{id: w.ID, from: "SUSPECT", to: "DEAD"})
			w.Status = "DEAD"
		}
	}
	r.mu.Unlock()

	for _, tr := range transitions {
		if err := r.store.SetWorkerStatus(ctx, tr.id, tr.to, nowMs); err != nil {
			r.log.Error("failed to persist worker transition", "worker_id", tr.id, "to", tr.to, "err", err)
		}
		r.notifyStatusChange(tr.id, tr.from, tr.to)
	}
}

// StartSweeper starts a background goroutine that sweeps every second.
func (r *Registry) StartSweeper(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.Sweep(ctx)
			}
		}
	}()
}

// Alive returns snapshots of all currently ALIVE workers.
func (r *Registry) Alive() []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := r.clock.Now()
	var alive []WorkerInfo
	for _, w := range r.workers {
		if w.Status == "ALIVE" {
			msAgo := now.Sub(w.LastSeen).Milliseconds()
			if msAgo < 0 {
				msAgo = 0
			}
			alive = append(alive, WorkerInfo{
				ID:            w.ID,
				Addr:          w.Addr,
				Status:        w.Status,
				LastSeen:      w.LastSeen,
				UsedBytes:     w.UsedBytes,
				Capacity:      w.Capacity,
				ChunkCount:    w.ChunkCount,
				InFlight:      w.InFlight.Load(),
				LastSeenMSAgo: msAgo,
			})
		}
	}
	return alive
}

// All returns snapshots of all registered workers.
func (r *Registry) All() []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := r.clock.Now()
	var all []WorkerInfo
	for _, w := range r.workers {
		msAgo := now.Sub(w.LastSeen).Milliseconds()
		if msAgo < 0 {
			msAgo = 0
		}
		all = append(all, WorkerInfo{
			ID:            w.ID,
			Addr:          w.Addr,
			Status:        w.Status,
			LastSeen:      w.LastSeen,
			UsedBytes:     w.UsedBytes,
			Capacity:      w.Capacity,
			ChunkCount:    w.ChunkCount,
			InFlight:      w.InFlight.Load(),
			LastSeenMSAgo: msAgo,
		})
	}
	// Sort by ID for deterministic output
	sort.Slice(all, func(i, j int) bool {
		return all[i].ID < all[j].ID
	})
	return all
}

// Get returns the snapshot of a specific worker if known.
func (r *Registry) Get(id string) (WorkerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	w, ok := r.workers[id]
	if !ok {
		return WorkerInfo{}, false
	}
	msAgo := r.clock.Now().Sub(w.LastSeen).Milliseconds()
	if msAgo < 0 {
		msAgo = 0
	}
	return WorkerInfo{
		ID:            w.ID,
		Addr:          w.Addr,
		Status:        w.Status,
		LastSeen:      w.LastSeen,
		UsedBytes:     w.UsedBytes,
		Capacity:      w.Capacity,
		ChunkCount:    w.ChunkCount,
		InFlight:      w.InFlight.Load(),
		LastSeenMSAgo: msAgo,
	}, true
}

// AddUsed optimistically increments (or decrements) the used bytes of a worker.
func (r *Registry) AddUsed(id string, delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if w, ok := r.workers[id]; ok {
		w.UsedBytes += delta
		if w.UsedBytes < 0 {
			w.UsedBytes = 0
		}
	}
}

// IncInFlight increments the in-flight request counter for a worker.
func (r *Registry) IncInFlight(id string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if w, ok := r.workers[id]; ok {
		w.InFlight.Add(1)
	}
}

// DecInFlight decrements the in-flight request counter for a worker.
func (r *Registry) DecInFlight(id string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if w, ok := r.workers[id]; ok {
		w.InFlight.Add(-1)
	}
}

// ReadCandidates orders candidates: ALIVE first, then SUSPECT, never DEAD; ties by lowest InFlight.
func (r *Registry) ReadCandidates(ids []string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	type candidate struct {
		id       string
		status   string
		inFlight int32
		randKey  int
	}

	var candidates []candidate
	for _, id := range ids {
		w, ok := r.workers[id]
		if !ok || w.Status == "DEAD" {
			continue
		}
		candidates = append(candidates, candidate{
			id:       id,
			status:   w.Status,
			inFlight: w.InFlight.Load(),
			randKey:  rand.Int(),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		// ALIVE before SUSPECT
		if candidates[i].status != candidates[j].status {
			return candidates[i].status == "ALIVE"
		}
		// Lowest in flight
		if candidates[i].inFlight != candidates[j].inFlight {
			return candidates[i].inFlight < candidates[j].inFlight
		}
		return candidates[i].randKey < candidates[j].randKey
	})

	result := make([]string, len(candidates))
	for i, c := range candidates {
		result[i] = c.id
	}
	return result
}
