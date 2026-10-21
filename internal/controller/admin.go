package controller

import (
	"net/http"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
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
	// Reconcile trigger is implemented in Phase 4; for now return OK
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
	w.WriteHeader(http.StatusAccepted)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]string{
		"status": "accepted",
		"worker": workerID,
	})
}
