// This file synchronizes worker inventories into controller metadata.
package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"objstore/internal/common/checksum"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

// InFlightChecker determines if an upload is actively in progress.
type InFlightChecker interface {
	IsInFlight(objectID string) bool
}

// InventorySyncer performs HDFS block-report style inventory synchronization.
type InventorySyncer struct {
	store    metadata.Store
	registry *registry.Registry
	client   workerclient.Client
	inFlight InFlightChecker
	log      *slog.Logger
	onWake   func()
}

// NewInventorySyncer creates a new InventorySyncer.
func NewInventorySyncer(
	store metadata.Store,
	reg *registry.Registry,
	client workerclient.Client,
	inFlight InFlightChecker,
	onWake func(),
	log *slog.Logger,
) *InventorySyncer {
	if log == nil {
		log = slog.Default()
	}
	return &InventorySyncer{
		store:    store,
		registry: reg,
		client:   client,
		inFlight: inFlight,
		onWake:   onWake,
		log:      log,
	}
}

// SyncWorker reconciles the reported inventory of workerID with the database.
func (s *InventorySyncer) SyncWorker(ctx context.Context, workerID string) error {
	wInfo, ok := s.registry.Get(workerID)
	if !ok {
		return errors.New("unknown worker for inventory sync: " + workerID)
	}

	ref := workerclient.WorkerRef{ID: workerID, Addr: wInfo.Addr}
	// Only older replica rows can be missing from the upcoming worker snapshot.
	known, err := s.store.ListChunkIDsForWorker(ctx, workerID)
	if err != nil {
		return err
	}
	inv, err := s.client.ListChunks(ctx, ref)
	if err != nil {
		s.log.Warn("inventory sync failed to list chunks", "worker_id", workerID, "err", err)
		return err
	}

	now := time.Now().UnixMilli()
	reported := make(map[string]string, len(inv.Chunks)+len(inv.Quarantined))
	for _, ch := range inv.Chunks {
		reported[ch.ChunkID] = ch.SHA256
	}
	for _, id := range inv.Quarantined {
		reported[id] = "" // Present on disk, but the scrubber rejected its contents.
	}
	for id, sha := range reported {
		if sep := strings.LastIndexByte(id, '_'); sep > 0 && s.inFlight != nil && s.inFlight.IsInFlight(id[:sep]) {
			continue
		}
		state, expected, _, err := s.store.GetChunkObjectInfo(ctx, id)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if state != "COMMITTED" {
			if err := s.client.DeleteChunk(ctx, ref, id); err != nil && !errors.Is(err, workerclient.ErrChunkNotFound) {
				return err
			}
			continue
		}
		replicaState := "CORRUPT"
		if sha != "" && checksum.EqualHex(sha, expected) {
			replicaState = "OK"
		}
		if err := s.store.UpsertReplica(ctx, id, workerID, replicaState, now); err != nil {
			return err
		}
	}
	if wInfo.Status == "ALIVE" {
		for _, id := range known {
			if _, present := reported[id]; !present {
				if err := s.store.DeleteReplica(ctx, id, workerID); err != nil {
					return err
				}
			}
		}
	}

	// Wake reconciler to re-evaluate cluster health
	if s.onWake != nil {
		s.onWake()
	}

	s.log.Info("inventory sync completed", "worker_id", workerID, "chunks", len(inv.Chunks), "quarantined", len(inv.Quarantined))
	return nil
}

// SyncAllAliveWorkers runs inventory sync across all currently ALIVE workers.
func (s *InventorySyncer) SyncAllAliveWorkers(ctx context.Context) {
	alive := s.registry.Alive()
	for _, w := range alive {
		select {
		case <-ctx.Done():
			return
		default:
			_ = s.SyncWorker(ctx, w.ID)
		}
	}
}
