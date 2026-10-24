package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"objstore/internal/controller/metadata"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

// GarbageCollector performs physical chunk deletion for objects in DELETING state.
type GarbageCollector struct {
	store    metadata.Store
	registry *registry.Registry
	client   workerclient.Client
	log      *slog.Logger
}

// NewGarbageCollector creates a new GarbageCollector.
func NewGarbageCollector(
	store metadata.Store,
	reg *registry.Registry,
	client workerclient.Client,
	log *slog.Logger,
) *GarbageCollector {
	if log == nil {
		log = slog.Default()
	}
	return &GarbageCollector{
		store:    store,
		registry: reg,
		client:   client,
		log:      log,
	}
}

// Run executes a garbage collection pass over objects in DELETING state.
func (gc *GarbageCollector) Run(ctx context.Context) error {
	items, err := gc.store.ListDeletingObjects(ctx)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	type replicaKey struct {
		chunkID  string
		workerID string
	}
	byObject := make(map[string][]replicaKey)
	for _, it := range items {
		if it.WorkerID != "" {
			byObject[it.ObjectID] = append(byObject[it.ObjectID], replicaKey{
				chunkID:  it.ChunkID,
				workerID: it.WorkerID,
			})
		} else if _, ok := byObject[it.ObjectID]; !ok {
			byObject[it.ObjectID] = []replicaKey{}
		}
	}

	aliveWorkers := make(map[string]bool)
	for _, w := range gc.registry.Alive() {
		aliveWorkers[w.ID] = true
	}

	for objID, replicas := range byObject {
		if len(replicas) == 0 {
			_ = gc.store.DeleteObject(ctx, objID)
			continue
		}

		var wg sync.WaitGroup
		sem := make(chan struct{}, 8)
		deletedKeys := sync.Map{}

		for _, rep := range replicas {
			wInfo, ok := gc.registry.Get(rep.workerID)
			if !ok || !aliveWorkers[rep.workerID] {
				// Worker is not ALIVE: skip physical delete now
				continue
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(rk replicaKey, addr string) {
				defer func() {
					<-sem
					wg.Done()
				}()

				ref := workerclient.WorkerRef{ID: rk.workerID, Addr: addr}
				err := gc.client.DeleteChunk(ctx, ref, rk.chunkID)
				if err == nil || errors.Is(err, workerclient.ErrChunkNotFound) {
					deletedKeys.Store(rk, true)
				}
			}(rep, wInfo.Addr)
		}
		wg.Wait()

		// Remove replica rows for successful deletes
		deletedKeys.Range(func(key, _ any) bool {
			rk := key.(replicaKey)
			_ = gc.store.DeleteReplica(ctx, rk.chunkID, rk.workerID)
			return true
		})

		// Check if any replicas remain on ALIVE workers
		hasAliveReplicas := false
		for _, rep := range replicas {
			if _, deleted := deletedKeys.Load(rep); !deleted {
				if aliveWorkers[rep.workerID] {
					hasAliveReplicas = true
					break
				}
			}
		}

		if !hasAliveReplicas {
			// All remaining replicas (if any) are on non-ALIVE workers:
			// Delete object row now (orphan sweep will clean files when workers return)
			gc.log.Info("garbage collector completed object deletion", "object_id", objID)
			_ = gc.store.DeleteObject(ctx, objID)
		}
	}

	return nil
}
