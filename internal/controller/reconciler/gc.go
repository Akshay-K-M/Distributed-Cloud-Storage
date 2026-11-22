// This file garbage-collects deleted objects and unreferenced worker chunks.
package reconciler

import (
	"context"
	"errors"
	"log/slog"

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
	// A worker that is offline will discard orphans on its next inventory.
	// Keep the tombstone if a reachable worker returns an error and retry
	// next pass. Repeating an already successful worker DELETE is safe.
	retry := make(map[string]bool)
	for _, item := range items {
		if _, seen := retry[item.ObjectID]; !seen {
			retry[item.ObjectID] = false
		}
		w, ok := gc.registry.Get(item.WorkerID)
		if !ok || w.Status != "ALIVE" {
			continue
		}
		ref := workerclient.WorkerRef{ID: w.ID, Addr: w.Addr}
		err := gc.client.DeleteChunk(ctx, ref, item.ChunkID)
		if err != nil && !errors.Is(err, workerclient.ErrChunkNotFound) {
			retry[item.ObjectID] = true
			gc.log.Warn("retrying chunk deletion next pass", "chunk_id", item.ChunkID, "worker", w.ID, "err", err)
		}
	}
	for id, failed := range retry {
		if !failed {
			if err := gc.store.DeleteObject(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
