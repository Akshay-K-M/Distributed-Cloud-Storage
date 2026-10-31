// This file serves the worker's authenticated chunk and maintenance endpoints.
package worker

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
	"objstore/internal/worker/chunkstore"
)

// Server serves the frozen worker internal API (section 5.2).
type Server struct {
	cfg      Config
	store    *chunkstore.Store
	scrubber *Scrubber
	log      *slog.Logger
}

// Handler returns the fully wrapped HTTP handler.
func Handler(cfg Config, store *chunkstore.Store, scrubber *Scrubber, log *slog.Logger) http.Handler {
	s := &Server{cfg: cfg, store: store, scrubber: scrubber, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /internal/chunks/{chunk_id}", s.putChunk)
	mux.HandleFunc("GET /internal/chunks/{chunk_id}", s.getChunk)
	mux.HandleFunc("DELETE /internal/chunks/{chunk_id}", s.deleteChunk)
	mux.HandleFunc("GET /internal/workers/{worker_id}/chunks", s.inventory)
	mux.HandleFunc("POST /internal/scrub", s.scrub)
	mux.HandleFunc("GET /healthz", s.healthz)
	return httpx.Chain(mux,
		httpx.Recover(log),
		httpx.RequestID,
		httpx.AccessLog(log),
		httpx.BearerAuth(cfg.Token),
	)
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	if !chunkstore.ValidID(id) {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid chunk id")
		return
	}
	cl := r.Header.Get("Content-Length")
	if cl == "" {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "Content-Length is required")
		return
	}
	size, err := strconv.ParseInt(cl, 10, 64)
	if err != nil || size < 0 {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid Content-Length")
		return
	}
	if size > s.cfg.ChunkSize {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "chunk exceeds CHUNK_SIZE")
		return
	}
	sha := r.Header.Get("X-Chunk-SHA256")
	if sha == "" {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "X-Chunk-SHA256 is required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.ChunkSize+1)
	created, err := s.store.Put(r.Context(), id, sha, size, r.Body)
	if errors.Is(err, chunkstore.ErrChecksumMismatch) {
		httpx.WriteError(w, r, api.CodeChecksumMismatch, "chunk checksum or length mismatch")
		return
	}
	if errors.Is(err, chunkstore.ErrInvalidID) {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid chunk id")
		return
	}
	if errors.Is(err, chunkstore.ErrTooLarge) {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "chunk exceeds CHUNK_SIZE")
		return
	}
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "chunk exceeds CHUNK_SIZE")
		return
	}
	if err != nil {
		s.log.Error("put chunk", "err", err, "chunk_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to store chunk")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	httpx.WriteJSON(w, status, api.ChunkPutResponse{ChunkID: id, Size: size, SHA256: sha})
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	rc, meta, err := s.store.Open(id)
	if errors.Is(err, chunkstore.ErrNotFound) || errors.Is(err, chunkstore.ErrInvalidID) {
		httpx.WriteError(w, r, api.CodeChunkNotFound, "chunk "+id+" not found")
		return
	}
	if err != nil {
		s.log.Error("open chunk", "err", err, "chunk_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to open chunk")
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Chunk-SHA256", meta.SHA256)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("Accept-Ranges", "none")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, rc)
	}
}

func (s *Server) deleteChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	if err := s.store.Delete(id); err != nil {
		s.log.Error("delete chunk", "err", err, "chunk_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to delete chunk")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) inventory(w http.ResponseWriter, r *http.Request) {
	wid := r.PathValue("worker_id")
	if wid != s.cfg.WorkerID {
		httpx.WriteError(w, r, api.CodeUnknownWorker, "unknown worker")
		return
	}
	metas, quarantined := s.store.Inventory()
	chunks := make([]api.InventoryChunk, 0, len(metas))
	for _, m := range metas {
		chunks = append(chunks, api.InventoryChunk{ChunkID: m.ChunkID, Size: m.Size, SHA256: m.SHA256})
	}
	if quarantined == nil {
		quarantined = []string{}
	}
	httpx.WriteJSON(w, http.StatusOK, api.Inventory{
		WorkerID:    s.cfg.WorkerID,
		Chunks:      chunks,
		Quarantined: quarantined,
	})
}

func (s *Server) scrub(w http.ResponseWriter, r *http.Request) {
	if s.scrubber != nil {
		s.scrubber.Trigger()
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, api.Healthz{Status: "ok", WorkerID: s.cfg.WorkerID})
}
