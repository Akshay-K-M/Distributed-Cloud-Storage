// This file fetches, verifies, and streams chunks with replica fallback.
package datapath

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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

	// Stage verified bytes so even an unavailable final chunk produces a JSON error.
	staged, err := os.CreateTemp("", "ocs-download-*")
	if err != nil {
		httpx.WriteError(w, r, api.CodeInternal, "cannot create temporary download file")
		return
	}
	defer os.Remove(staged.Name())
	defer staged.Close()

	for _, chunk := range objDetail.Chunks {
		data, err := d.fetchChunk(r.Context(), chunk)
		if err != nil {
			httpx.WriteError(w, r, api.CodeStorageUnavailable, "chunk unavailable")
			return
		}
		if _, err := staged.Write(data); err != nil {
			httpx.WriteError(w, r, api.CodeInternal, "cannot stage download")
			return
		}
	}
	if _, err := staged.Seek(0, io.SeekStart); err != nil {
		httpx.WriteError(w, r, api.CodeInternal, "cannot read staged download")
		return
	}

	w.Header().Set("Content-Type", objDetail.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(objDetail.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf("%q", objDetail.ChecksumSHA256))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(objDetail.Name)))

	if _, err := io.Copy(w, staged); err != nil {
		d.log.Info("download interrupted", "object_id", id, "err", err)
	}
}

func (d *DataPath) fetchChunk(ctx context.Context, chunk api.ChunkInfo) ([]byte, error) {
	var okWorkers []string
	for _, rep := range chunk.Replicas {
		if rep.State == "OK" {
			okWorkers = append(okWorkers, rep.WorkerID)
		}
	}

	candidates := d.registry.ReadCandidates(okWorkers)

	for _, workerID := range candidates {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		wInfo, ok := d.registry.Get(workerID)
		if !ok {
			continue
		}

		d.registry.IncInFlight(workerID)
		body, _, _, err := d.client.GetChunk(ctx, workerclient.WorkerRef{ID: wInfo.ID, Addr: wInfo.Addr}, chunk.ChunkID)

		if err != nil {
			d.registry.DecInFlight(workerID)
			d.log.Warn("replica unavailable", "worker_id", workerID, "chunk_id", chunk.ChunkID, "err", err)
			continue
		}

		b, readErr := io.ReadAll(io.LimitReader(body, chunk.Size+1))
		body.Close()
		d.registry.DecInFlight(workerID)

		if readErr != nil {
			continue
		}
		if int64(len(b)) != chunk.Size || !checksum.EqualHex(checksum.SHA256Hex(b), chunk.SHA256) {
			d.log.Warn("replica failed integrity check", "worker_id", workerID, "chunk_id", chunk.ChunkID)
			_ = d.store.SetReplicaState(ctx, chunk.ChunkID, workerID, "CORRUPT", time.Now().UnixMilli())
			if d.onReconcileWake != nil {
				d.onReconcileWake()
			}
			continue
		}
		return b, nil
	}

	return nil, errChunkUnavailable
}
