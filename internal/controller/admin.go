// This file serves controller health, status, reconcile, and scrub endpoints.
package controller

import (
	"context"
	"net/http"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
	"objstore/internal/workerclient"
)

func (s *Server) handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	objCounts, err := s.store.ObjectCounts(r.Context())
	if err != nil {
		s.log.Error("admin status: object counts", "err", err)
		httpx.WriteError(w, r, api.CodeInternal, "failed to compute object counts")
		return
	}

	chunkCounts, err := s.store.ChunkCounts(r.Context())
	if err != nil {
		s.log.Error("admin status: chunk counts", "err", err)
		httpx.WriteError(w, r, api.CodeInternal, "failed to compute chunk counts")
		return
	}

	// Reconciler status supplies live replica-health counts when available.
	if s.reconciler != nil {
		if under, over, corrupt, lost, err := s.reconciler.StatusCounts(r.Context()); err == nil {
			chunkCounts.UnderReplicated = under
			chunkCounts.OverReplicated = over
			chunkCounts.CorruptReplicas = corrupt
			chunkCounts.Lost = lost
		}
	}

	allWorkers := s.registry.All()
	workerInfos := make([]api.WorkerInfo, 0, len(allWorkers))
	for _, w := range allWorkers {
		workerInfos = append(workerInfos, api.WorkerInfo{
			WorkerID:      w.ID,
			Status:        w.Status,
			Address:       w.Addr,
			UsedBytes:     w.UsedBytes,
			ChunkCount:    w.ChunkCount,
			LastSeenMSAgo: w.LastSeenMSAgo,
		})
	}

	status := api.AdminStatus{
		Config: api.AdminConfig{
			ReplicationFactor: s.cfg.ReplicationFactor,
			WriteAcks:         s.cfg.WriteAcks,
			ChunkSize:         s.cfg.ChunkSize,
		},
		Workers: workerInfos,
		Objects: objCounts,
		Chunks:  chunkCounts,
	}

	httpx.WriteJSON(w, http.StatusOK, status)
}

func (s *Server) handleAdminReconcile(w http.ResponseWriter, r *http.Request) {
	if s.reconciler != nil {
		s.reconciler.RunPass(r.Context())
		go s.reconciler.SyncAllWorkers(context.Background())
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleAdminScrub(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "worker query parameter is required")
		return
	}

	if s.wClient != nil {
		wInfo, ok := s.registry.Get(workerID)
		if ok {
			_ = s.wClient.Scrub(r.Context(), workerclient.WorkerRef{ID: workerID, Addr: wInfo.Addr})
		}
	}

	w.WriteHeader(http.StatusAccepted)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]string{
		"status": "accepted",
		"worker": workerID,
	})
}
