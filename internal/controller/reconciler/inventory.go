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
	inv, err := s.client.ListChunks(ctx, ref)
	if err != nil {
		s.log.Warn("inventory sync failed to list chunks", "worker_id", workerID, "err", err)
		return err
	}

	now := time.Now().UnixMilli()
	reported := make(map[string]bool)

	// 1. Process reported chunks
	for _, ch := range inv.Chunks {
		reported[ch.ChunkID] = true

		objState, expectedSHA, objID, err := s.store.GetChunkObjectInfo(ctx, ch.ChunkID)
		if err == nil && objState == "COMMITTED" {
			// Known committed chunk: adopt or update state
			if checksum.EqualHex(ch.SHA256, expectedSHA) {
				_ = s.store.UpsertReplica(ctx, ch.ChunkID, workerID, "OK", now)
			} else {
				s.log.Warn("inventory sync found corrupted chunk", "worker_id", workerID, "chunk_id", ch.ChunkID)
				_ = s.store.UpsertReplica(ctx, ch.ChunkID, workerID, "CORRUPT", now)
			}
		} else {
			// Chunk is unknown in DB or its object is not COMMITTED
			candObjID := objID
			if candObjID == "" {
				if lastIdx := strings.LastIndex(ch.ChunkID, "_"); lastIdx > 0 {
					candObjID = ch.ChunkID[:lastIdx]
				}
			}
			if candObjID != "" && s.inFlight != nil && s.inFlight.IsInFlight(candObjID) {
				// Upload in progress: do not delete
				continue
			}

			// Orphan chunk: purge from worker
			s.log.Info("inventory sync deleting orphan chunk from worker", "worker_id", workerID, "chunk_id", ch.ChunkID)
			_ = s.client.DeleteChunk(ctx, ref, ch.ChunkID)
		}
	}

	// 2. Detect lost chunks on the worker (rows in DB not reported by alive worker)
	if wInfo.Status == "ALIVE" {
		dbChunkIDs, err := s.store.ListChunkIDsForWorker(ctx, workerID)
		if err == nil {
			for _, chID := range dbChunkIDs {
				if !reported[chID] {
					s.log.Warn("inventory sync detected missing chunk on worker, deleting replica row", "worker_id", workerID, "chunk_id", chID)
					_ = s.store.DeleteReplica(ctx, chID, workerID)
				}
			}
		}
	}

	// 3. Mark quarantined chunks as CORRUPT
	for _, qID := range inv.Quarantined {
		_ = s.store.SetReplicaState(ctx, qID, workerID, "CORRUPT", now)
	}

	// 4. Wake reconciler to re-evaluate cluster health
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
