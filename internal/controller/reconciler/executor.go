package reconciler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"objstore/internal/common/checksum"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

// Executor runs planned reconciliation actions using bounded concurrency.
type Executor struct {
	store       metadata.Store
	registry    *registry.Registry
	placer      *placement.Placer
	client      workerclient.Client
	concurrency int
	log         *slog.Logger
}

// NewExecutor creates a new Executor.
func NewExecutor(
	store metadata.Store,
	reg *registry.Registry,
	placer *placement.Placer,
	client workerclient.Client,
	concurrency int,
	log *slog.Logger,
) *Executor {
	if concurrency < 1 {
		concurrency = 2
	}
	if log == nil {
		log = slog.Default()
	}
	return &Executor{
		store:       store,
		registry:    reg,
		placer:      placer,
		client:      client,
		concurrency: concurrency,
		log:         log,
	}
}

// Execute processes all actions in order using up to concurrency parallel workers.
func (e *Executor) Execute(ctx context.Context, actions []Action) error {
	sem := make(chan struct{}, e.concurrency)
	var wg sync.WaitGroup

	for _, act := range actions {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(action Action) {
			defer func() {
				<-sem
				wg.Done()
			}()

			switch a := action.(type) {
			case RepairChunk:
				if err := e.repairChunk(ctx, a); err != nil {
					e.log.Warn("repair chunk failed", "chunk_id", a.Chunk.ChunkID, "err", err)
				}
			case TrimReplica:
				if err := e.trimReplica(ctx, a); err != nil {
					e.log.Warn("trim replica failed", "chunk_id", a.ChunkID, "worker_id", a.WorkerID, "err", err)
				}
			case DeleteCorrupt:
				if err := e.deleteCorrupt(ctx, a); err != nil {
					e.log.Warn("delete corrupt replica failed", "chunk_id", a.ChunkID, "worker_id", a.WorkerID, "err", err)
				}
			}
		}(act)
	}

	wg.Wait()
	return nil
}

func (e *Executor) repairChunk(ctx context.Context, a RepairChunk) error {
	now := time.Now().UnixMilli()

	// 1. Pick target worker
	picked, err := e.placer.Pick(1, a.Exclude, a.Chunk.Size)
	if err != nil || len(picked) == 0 {
		// Fallback: exclude only workers with OK replicas (re-use a worker holding a CORRUPT copy)
		excludeOK := make(map[string]bool)
		for _, rep := range a.Chunk.Replicas {
			if rep.State == "OK" {
				excludeOK[rep.WorkerID] = true
			}
		}
		picked, err = e.placer.Pick(1, excludeOK, a.Chunk.Size)
		if err != nil || len(picked) == 0 {
			return fmt.Errorf("no target worker available to place replica: %w", err)
		}
	}

	target := picked[0]
	targetRef := workerclient.WorkerRef{ID: target.ID, Addr: target.Addr}

	// 2. Select source worker
	sourceCandidates := e.registry.ReadCandidates(a.Sources)
	if len(sourceCandidates) == 0 {
		// Even if all are DEAD, try available sources as fallback
		sourceCandidates = a.Sources
	}

	var verifiedData []byte
	var chosenSource string

	for _, sourceID := range sourceCandidates {
		wInfo, ok := e.registry.Get(sourceID)
		if !ok {
			continue
		}
		sourceRef := workerclient.WorkerRef{ID: sourceID, Addr: wInfo.Addr}

		rc, size, _, err := e.client.GetChunk(ctx, sourceRef, a.Chunk.ChunkID)
		if err != nil {
			e.log.Warn("failed to fetch chunk from source replica", "source", sourceID, "chunk_id", a.Chunk.ChunkID, "err", err)
			continue
		}

		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			continue
		}

		if int64(len(data)) != size && size > 0 {
			continue
		}

		// Verify SHA-256 against metadata
		sumHex := checksum.SHA256Hex(data)
		if !checksum.EqualHex(sumHex, a.Chunk.SHA) {
			e.log.Warn("source chunk checksum mismatch during repair", "source", sourceID, "chunk_id", a.Chunk.ChunkID)
			_ = e.store.SetReplicaState(ctx, a.Chunk.ChunkID, sourceID, "CORRUPT", now)
			continue
		}

		verifiedData = data
		chosenSource = sourceID
		break
	}

	if verifiedData == nil {
		e.placer.Release(target.ID, a.Chunk.Size)
		return errors.New("no healthy verified source replica found")
	}

	// 3. Put chunk to target
	err = e.client.PutChunk(ctx, targetRef, a.Chunk.ChunkID, a.Chunk.SHA, int64(len(verifiedData)), bytes.NewReader(verifiedData))
	if err != nil {
		e.placer.Release(target.ID, a.Chunk.Size)
		return fmt.Errorf("failed to put repaired chunk to target %s: %w", target.ID, err)
	}

	// 4. Conditional insert into DB
	inserted, err := e.store.InsertReplicaIfCommitted(ctx, a.Chunk.ChunkID, target.ID, now)
	if err != nil {
		e.log.Error("failed to insert replica record after repair copy", "chunk_id", a.Chunk.ChunkID, "target", target.ID, "err", err)
		return err
	}

	if !inserted {
		// Object was deleted or uncommitted while copy was in flight -> delete orphan copy
		e.log.Info("object deleted while repair in flight, removing orphan chunk", "chunk_id", a.Chunk.ChunkID, "target", target.ID)
		_ = e.client.DeleteChunk(ctx, targetRef, a.Chunk.ChunkID)
		e.placer.Release(target.ID, a.Chunk.Size)
		return nil
	}

	e.log.Info("chunk repaired successfully", "chunk_id", a.Chunk.ChunkID, "from", chosenSource, "to", target.ID)
	return nil
}

func (e *Executor) trimReplica(ctx context.Context, a TrimReplica) error {
	wInfo, ok := e.registry.Get(a.WorkerID)
	if !ok {
		// Unknown worker: remove stale DB row
		return e.store.DeleteReplica(ctx, a.ChunkID, a.WorkerID)
	}

	ref := workerclient.WorkerRef{ID: a.WorkerID, Addr: wInfo.Addr}
	if err := e.client.DeleteChunk(ctx, ref, a.ChunkID); err != nil && !errors.Is(err, workerclient.ErrChunkNotFound) {
		// If worker is unreachable, leave row for next pass
		return err
	}

	return e.store.DeleteReplica(ctx, a.ChunkID, a.WorkerID)
}

func (e *Executor) deleteCorrupt(ctx context.Context, a DeleteCorrupt) error {
	wInfo, ok := e.registry.Get(a.WorkerID)
	if ok {
		ref := workerclient.WorkerRef{ID: a.WorkerID, Addr: wInfo.Addr}
		_ = e.client.DeleteChunk(ctx, ref, a.ChunkID)
	}
	return e.store.DeleteReplica(ctx, a.ChunkID, a.WorkerID)
}
