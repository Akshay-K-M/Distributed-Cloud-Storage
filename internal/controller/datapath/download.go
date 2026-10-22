package datapath

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/checksum"
	"objstore/internal/common/httpx"
	"objstore/internal/controller/metadata"
	"objstore/internal/workerclient"
)

// Download handles GET /api/v1/objects/{object_id}.
func (d *DataPath) Download(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	objDetail, err := d.store.GetObjectWithChunks(r.Context(), id)
	if errors.Is(err, metadata.ErrNotFound) {
		httpx.WriteError(w, r, api.CodeObjectNotFound, "object not found")
		return
	}
	if err != nil {
		d.log.Error("get object with chunks failed", "err", err, "object_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to retrieve object")
		return
	}

	if len(objDetail.Chunks) == 0 {
		httpx.WriteError(w, r, api.CodeInternal, "object has no chunks")
		return
	}

	// 1. Fetch and verify chunk 0 before writing ANY headers
	chunk0, fallbacks0, err := d.fetchChunk(r.Context(), objDetail.Chunks[0])
	if err != nil {
		d.log.Warn("chunk 0 unavailable on all replicas", "err", err, "object_id", id)
		httpx.WriteError(w, r, api.CodeStorageUnavailable, "chunk unavailable")
		return
	}

	totalFallbacks := fallbacks0

	// 2. Set headers
	w.Header().Set("Content-Type", objDetail.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(objDetail.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf("%q", objDetail.ChecksumSHA256))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(objDetail.Name)))

	// 3. Write chunk 0
	if _, err := w.Write(chunk0); err != nil {
		d.log.Info("client aborted during chunk 0 streaming", "err", err, "object_id", id)
		return
	}

	if len(objDetail.Chunks) == 1 {
		d.log.Info("download completed", "object_id", id, "name", objDetail.Name, "bytes", objDetail.Size, "chunks", 1, "fallbacks", totalFallbacks)
		return
	}

	// 4. Prefetch remaining chunks (1..N-1) using bounded pipeline
	type futureChunk struct {
		idx       int
		data      []byte
		fallbacks int
		err       error
	}

	prefetchCap := d.cfg.DownloadPrefetch
	if prefetchCap < 1 {
		prefetchCap = 1
	}

	futureCh := make(chan chan futureChunk, prefetchCap)
	producerCtx, cancelProducer := context.WithCancel(r.Context())
	defer cancelProducer()

	go func() {
		defer close(futureCh)
		for i := 1; i < len(objDetail.Chunks); i++ {
			ci := objDetail.Chunks[i]
			f := make(chan futureChunk, 1)

			select {
			case <-producerCtx.Done():
				return
			case futureCh <- f:
			}

			go func(chunk api.ChunkInfo, ch chan futureChunk) {
				data, fallbacks, fetchErr := d.fetchChunk(producerCtx, chunk)
				ch <- futureChunk{idx: chunk.Index, data: data, fallbacks: fallbacks, err: fetchErr}
			}(ci, f)
		}
	}()

	// 5. Consume remaining chunks in order
	for f := range futureCh {
		select {
		case <-r.Context().Done():
			d.log.Info("client aborted during download streaming", "object_id", id)
			return
		case res := <-f:
			if res.err != nil {
				d.log.Error("chunk read failure after headers sent; aborting connection", "chunk_idx", res.idx, "err", res.err, "object_id", id)
				panic(http.ErrAbortHandler)
			}
			totalFallbacks += res.fallbacks
			if _, err := w.Write(res.data); err != nil {
				d.log.Info("client aborted during write", "err", err, "object_id", id)
				return
			}
		}
	}

	d.log.Info("download completed", "object_id", id, "name", objDetail.Name, "bytes", objDetail.Size, "chunks", len(objDetail.Chunks), "fallbacks", totalFallbacks)
}

func (d *DataPath) fetchChunk(ctx context.Context, chunk api.ChunkInfo) ([]byte, int, error) {
	var okWorkers []string
	for _, rep := range chunk.Replicas {
		if rep.State == "OK" {
			okWorkers = append(okWorkers, rep.WorkerID)
		}
	}

	candidates := d.registry.ReadCandidates(okWorkers)
	fallbacks := 0

	for _, workerID := range candidates {
		if ctx.Err() != nil {
			return nil, fallbacks, ctx.Err()
		}

		wInfo, ok := d.registry.Get(workerID)
		if !ok {
			continue
		}

		d.registry.IncInFlight(workerID)
		body, size, _, err := d.client.GetChunk(ctx, workerclient.WorkerRef{ID: wInfo.ID, Addr: wInfo.Addr}, chunk.ChunkID)
		d.registry.DecInFlight(workerID)

		if err != nil {
			d.log.Warn("worker GetChunk failed; marking replica CORRUPT", "worker_id", workerID, "chunk_id", chunk.ChunkID, "err", err)
			_ = d.store.SetReplicaState(context.Background(), chunk.ChunkID, workerID, "CORRUPT", time.Now().UnixMilli())
			if d.onReconcileWake != nil {
				d.onReconcileWake()
			}
			fallbacks++
			continue
		}

		b, readErr := io.ReadAll(io.LimitReader(body, chunk.Size+1))
		body.Close()

		if readErr != nil || int64(len(b)) != chunk.Size {
			d.log.Warn("chunk read error or size mismatch; marking replica CORRUPT", "worker_id", workerID, "chunk_id", chunk.ChunkID, "expected", chunk.Size, "got", len(b), "err", readErr)
			_ = d.store.SetReplicaState(context.Background(), chunk.ChunkID, workerID, "CORRUPT", time.Now().UnixMilli())
			if d.onReconcileWake != nil {
				d.onReconcileWake()
			}
			fallbacks++
			continue
		}

		computedSHA := checksum.SHA256Hex(b)
		if !checksum.EqualHex(computedSHA, chunk.SHA256) {
			d.log.Warn("chunk checksum mismatch, falling back", "worker_id", workerID, "chunk_id", chunk.ChunkID, "expected", chunk.SHA256, "computed", computedSHA)
			_ = d.store.SetReplicaState(context.Background(), chunk.ChunkID, workerID, "CORRUPT", time.Now().UnixMilli())
			if d.onReconcileWake != nil {
				d.onReconcileWake()
			}
			fallbacks++
			continue
		}

		_ = size
		return b, fallbacks, nil
	}

	return nil, fallbacks, errChunkUnavailable
}
