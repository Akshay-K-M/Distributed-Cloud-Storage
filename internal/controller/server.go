package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

// DataPath interface allows Phase 3A data plane to plug in seamlessly.
type DataPath interface {
	Upload(w http.ResponseWriter, r *http.Request)
	Download(w http.ResponseWriter, r *http.Request)
}

// ReconcilerProvider abstracts the reconciler for the server.
type ReconcilerProvider interface {
	RunPass(ctx context.Context)
	StatusCounts(ctx context.Context) (under, over, corrupt, lost int, err error)
	SyncWorker(ctx context.Context, workerID string) error
	SyncAllWorkers(ctx context.Context)
	Wake()
}

// Server is the HTTP server for the controller.
type Server struct {
	cfg        Config
	store      metadata.Store
	registry   *registry.Registry
	placer     *placement.Placer
	dataPath   DataPath
	reconciler ReconcilerProvider
	wClient    workerclient.Client
	log        *slog.Logger
	onSyncInv  func(workerID string) // inventory sync hook
}

// NewServer creates a new controller Server.
func NewServer(cfg Config, store metadata.Store, reg *registry.Registry, placer *placement.Placer, dp DataPath, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		cfg:      cfg,
		store:    store,
		registry: reg,
		placer:   placer,
		dataPath: dp,
		log:      log,
	}
}

// SetDataPath attaches the Phase 3A data path implementation.
func (s *Server) SetDataPath(dp DataPath) {
	s.dataPath = dp
}

// SetReconciler attaches the reconciler to the server.
func (s *Server) SetReconciler(r ReconcilerProvider) {
	s.reconciler = r
}

// SetWorkerClient attaches the worker client for admin calls.
func (s *Server) SetWorkerClient(c workerclient.Client) {
	s.wClient = c
}

// SetInventorySyncHook sets the callback hook for inventory sync when a worker registers.
func (s *Server) SetInventorySyncHook(hook func(workerID string)) {
	s.onSyncInv = hook
}

// Handler returns the HTTP handler with routes and middleware configured.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Public API (5.1)
	mux.HandleFunc("POST /api/v1/objects", s.handleUpload)
	mux.HandleFunc("GET /api/v1/objects/{object_id}", s.handleDownload)
	mux.HandleFunc("GET /api/v1/objects", s.handleList)
	mux.HandleFunc("GET /api/v1/objects/{object_id}/metadata", s.handleGetMetadata)
	mux.HandleFunc("PATCH /api/v1/objects/{object_id}/metadata", s.handlePatchMetadata)
	mux.HandleFunc("DELETE /api/v1/objects/{object_id}", s.handleDelete)

	// Internal API (5.3)
	mux.HandleFunc("POST /internal/workers/register", s.handleWorkerRegister)
	mux.HandleFunc("POST /internal/workers/heartbeat", s.handleWorkerHeartbeat)

	// Ops and Admin (5.1)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/v1/admin/status", s.handleAdminStatus)
	mux.HandleFunc("POST /api/v1/admin/reconcile", s.handleAdminReconcile)
	mux.HandleFunc("POST /api/v1/admin/scrub", s.handleAdminScrub)

	return httpx.Chain(mux,
		httpx.Recover(s.log),
		httpx.RequestID,
		httpx.AccessLog(s.log),
		httpx.BearerAuth(s.cfg.ClusterToken),
	)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if s.dataPath != nil {
		s.dataPath.Upload(w, r)
		return
	}
	w.WriteHeader(http.StatusNotImplemented)
	httpx.WriteJSON(w, http.StatusNotImplemented, api.ErrorBody{
		Error: api.ErrorDetail{
			Code:      api.CodeInternal,
			Message:   "upload is not implemented (Phase 3A)",
			RequestID: httpx.RequestIDFrom(r.Context()),
		},
	})
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if s.dataPath != nil {
		s.dataPath.Download(w, r)
		return
	}
	w.WriteHeader(http.StatusNotImplemented)
	httpx.WriteJSON(w, http.StatusNotImplemented, api.ErrorBody{
		Error: api.ErrorDetail{
			Code:      api.CodeInternal,
			Message:   "download is not implemented (Phase 3A)",
			RequestID: httpx.RequestIDFrom(r.Context()),
		},
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	cursor := q.Get("cursor")
	exactName := q.Get("name")

	limit := 100
	if lStr := q.Get("limit"); lStr != "" {
		if n, err := strconv.Atoi(lStr); err == nil && n > 0 {
			limit = n
		}
	}

	params := metadata.ListParams{
		Prefix:    prefix,
		Delimiter: delimiter,
		Limit:     limit,
		Cursor:    cursor,
		ExactName: exactName,
	}

	res, err := s.store.List(r.Context(), params)
	if errors.Is(err, metadata.ErrInvalidRequest) {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid pagination cursor")
		return
	}
	if err != nil {
		s.log.Error("list objects", "err", err)
		httpx.WriteError(w, r, api.CodeInternal, "failed to list objects")
		return
	}

	httpx.WriteJSON(w, http.StatusOK, res)
}

func (s *Server) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	detail, err := s.store.GetObjectWithChunks(r.Context(), id)
	if errors.Is(err, metadata.ErrNotFound) {
		httpx.WriteError(w, r, api.CodeObjectNotFound, "object not found")
		return
	}
	if err != nil {
		s.log.Error("get object metadata", "err", err, "object_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to get object metadata")
		return
	}

	httpx.WriteJSON(w, http.StatusOK, detail)
}

func (s *Server) handlePatchMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")

	var raw map[string]any
	dec := json.NewDecoder(io.LimitReader(r.Body, 64*1024))
	if err := dec.Decode(&raw); err != nil {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid JSON payload")
		return
	}

	patch := make(map[string]*string)
	for k, v := range raw {
		if v == nil {
			patch[k] = nil
		} else if strVal, ok := v.(string); ok {
			patch[k] = &strVal
		} else {
			httpx.WriteError(w, r, api.CodeInvalidRequest, "custom metadata values must be strings or null")
			return
		}
	}

	updated, err := s.store.PatchMetadata(r.Context(), id, patch)
	if errors.Is(err, metadata.ErrNotFound) {
		httpx.WriteError(w, r, api.CodeObjectNotFound, "object not found")
		return
	}
	if errors.Is(err, metadata.ErrInvalidRequest) {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid metadata key format, value length, or key limit exceeded")
		return
	}
	if err != nil {
		s.log.Error("patch object metadata", "err", err, "object_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to update metadata")
		return
	}

	httpx.WriteJSON(w, http.StatusOK, updated)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	err := s.store.MarkDeleting(r.Context(), id)
	if errors.Is(err, metadata.ErrNotFound) {
		httpx.WriteError(w, r, api.CodeObjectNotFound, "object not found or already deleting")
		return
	}
	if err != nil {
		s.log.Error("delete object", "err", err, "object_id", id)
		httpx.WriteError(w, r, api.CodeInternal, "failed to delete object")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	var req api.RegisterRequest
	if err := httpx.DecodeJSON(r.Body, &req); err != nil || req.WorkerID == "" || req.Address == "" {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid register request body")
		return
	}

	if err := s.registry.Register(r.Context(), req); err != nil {
		s.log.Error("register worker", "err", err, "worker_id", req.WorkerID)
		httpx.WriteError(w, r, api.CodeInternal, "failed to register worker")
		return
	}

	if s.reconciler != nil {
		go func(wID string) {
			_ = s.reconciler.SyncWorker(context.Background(), wID)
		}(req.WorkerID)
	} else if s.onSyncInv != nil {
		go s.onSyncInv(req.WorkerID)
	}

	httpx.WriteJSON(w, http.StatusOK, api.RegisterResponse{
		HeartbeatIntervalMS: s.cfg.HeartbeatInterval.Milliseconds(),
	})
}

func (s *Server) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req api.HeartbeatRequest
	if err := httpx.DecodeJSON(r.Body, &req); err != nil || req.WorkerID == "" {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid heartbeat request body")
		return
	}

	err := s.registry.Heartbeat(r.Context(), req)
	if errors.Is(err, registry.ErrUnknownWorker) {
		httpx.WriteError(w, r, api.CodeUnknownWorker, "unknown worker")
		return
	}
	if err != nil {
		s.log.Error("heartbeat", "err", err, "worker_id", req.WorkerID)
		httpx.WriteError(w, r, api.CodeInternal, "failed to process heartbeat")
		return
	}

	now := r.Context().Value(httpx.RequestIDFrom(r.Context()))
	_ = now
	// Mark any reported corrupt replicas in metadata store
	if len(req.CorruptChunks) > 0 {
		nowMs := s.cfg.HeartbeatInterval.Milliseconds() // or time.Now().UnixMilli()
		for _, chID := range req.CorruptChunks {
			_ = s.store.SetReplicaState(r.Context(), chID, req.WorkerID, "CORRUPT", nowMs)
		}
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, api.Healthz{Status: "ok"})
}
