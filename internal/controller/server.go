// This file wires public and internal controller routes with middleware.
// Read this as the controller's API map: which request reaches which operation.
// Object metadata requests use SQLite; uploads/downloads delegate to DataPath;
// worker reports update the registry and trigger background maintenance.
// The admin handlers registered here are implemented separately in admin.go.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

// DataPath is the controller's streaming data-plane boundary.
// Keeping byte transfer behind this boundary lets routing stay separate from
// chunking, replica acknowledgements, checksum verification, and read fallback.
type DataPath interface {
	Upload(w http.ResponseWriter, r *http.Request)
	Download(w http.ResponseWriter, r *http.Request)
}

// ReconcilerProvider abstracts the reconciler for the server.
// Lets API handlers request maintenance without implementing its scheduling,
// repair planning, inventory comparison, or physical cleanup themselves.
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
	dataPath   DataPath
	reconciler ReconcilerProvider
	wClient    workerclient.Client
	log        *slog.Logger
}

// NewServer creates a new controller Server.
// Receives components assembled by cmd/controller/main.go; this constructor
// does not open a listening port or start workers/background loops.
func NewServer(cfg Config, store metadata.Store, reg *registry.Registry, dp DataPath, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		cfg:      cfg,
		store:    store,
		registry: reg,
		dataPath: dp,
		log:      log,
	}
}

// SetReconciler attaches the reconciler to the server.
// Enables inventory synchronization on registration and maintenance admin calls.
func (s *Server) SetReconciler(r ReconcilerProvider) {
	s.reconciler = r
}

// SetWorkerClient attaches the worker client for admin calls.
// Allows the admin scrub handler to forward a scrub request to a chosen worker.
func (s *Server) SetWorkerClient(c workerclient.Client) {
	s.wClient = c
}

// Handler returns the HTTP handler with routes and middleware configured.
// Matches the request's HTTP method and URL to a handler below. For example,
// GET /objects/{id} fetches bytes while GET /objects/{id}/metadata fetches JSON.
// Public/internal distinguish intended callers; in this implementation only
// /internal/* requires the shared bearer token. Admin handlers live in admin.go.
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

	// Wrap routes with common request handling: recover panics, assign request
	// IDs, log accesses, and check the token on internal routes.
	return httpx.Chain(mux,
		httpx.Recover(s.log),
		httpx.RequestID,
		httpx.AccessLog(s.log),
		httpx.BearerAuth(s.cfg.ClusterToken),
	)
}

// handleUpload accepts POST /api/v1/objects and delegates the multipart stream
// to DataPath.Upload. That component splits chunks, writes replicas, and commits
// metadata before returning 201. Example caller: ocs cp a.txt obj:notes/a.txt.
// This wrapper itself neither buffers the object nor selects worker destinations.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if s.dataPath != nil {
		s.dataPath.Upload(w, r)
		return
	}
	httpx.WriteJSON(w, http.StatusNotImplemented, api.ErrorBody{
		Error: api.ErrorDetail{
			Code:      api.CodeInternal,
			Message:   "upload is not configured",
			RequestID: httpx.RequestIDFrom(r.Context()),
		},
	})
}

// handleDownload delegates GET /api/v1/objects/{id} to DataPath.Download.
// It looks up chunk locations, fetches and verifies chunks from workers, falls
// back between replicas when possible, and returns ordered object bytes.
// Example callers: ocs cat and remote-to-local ocs cp.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if s.dataPath != nil {
		s.dataPath.Download(w, r)
		return
	}
	httpx.WriteJSON(w, http.StatusNotImplemented, api.ErrorBody{
		Error: api.ErrorDetail{
			Code:      api.CodeInternal,
			Message:   "download is not configured",
			RequestID: httpx.RequestIDFrom(r.Context()),
		},
	})
}

// handleList answers GET /api/v1/objects using SQLite only; no worker is polled.
// Supports prefix grouping and pagination for ls, plus exact-name lookup for
// resolving obj:name to an ID. Example: ?name=notes/a.txt returns its summary,
// whereas ?prefix=notes/&delimiter=/ returns a folder-like listing page.
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	cursor := q.Get("cursor")
	exactName := q.Get("name")

	// Read listing options; the metadata store applies its own maximum page size.
	limit := 100
	if lStr := q.Get("limit"); lStr != "" {
		n, err := strconv.Atoi(lStr)
		if err != nil || n <= 0 {
			httpx.WriteError(w, r, api.CodeInvalidRequest, "limit must be a positive integer")
			return
		}
		limit = n
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

// handleGetMetadata returns an object's recorded properties and chunk replica
// layout as JSON, or 404 if absent. Used by stat and meta get. Reads SQLite;
// it does not download chunk bytes or freshly verify workers' files. Therefore
// an OK replica in this response can belong to a worker currently marked DEAD.
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

// handlePatchMetadata changes user-defined labels without transferring file data.
// Example body: {"owner":"alice","album":null} sets owner and removes album;
// omitted labels stay unchanged. It validates values, updates SQLite through
// the metadata store, and returns updated object detail. Missing object -> 404.
func (s *Server) handlePatchMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")

	var patch map[string]*string
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil || json.Unmarshal(body, &patch) != nil || patch == nil {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "expected a JSON object of string or null values (64 KiB maximum)")
		return
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

// handleDelete marks an object DELETING in SQLite and returns 204 immediately.
// Reads/listings no longer expose it; the background reconciler's garbage
// collector removes worker copies later. This request does not wait for every
// worker, which could be offline. Returned workers' orphan files are handled by
// inventory cleanup. Missing/already-deleting objects return 404.
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

// handleWorkerRegister receives the worker's initial introduction: ID, reachable
// address, capacity, and usage. The WORKER initiates this request; the controller
// doesn't need a hardcoded list of four worker addresses to discover them.
// It records the worker as ALIVE, starts inventory synchronization in background,
// and replies with the heartbeat interval (2 seconds in the Compose setup).
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

	// Registration causes a request in the opposite direction: controller GETs
	// /internal/workers/{id}/chunks from the worker. Inventory comparison updates
	// known replicas and deletes orphan files, while protecting active uploads.
	// The registration reply need not wait for this potentially slower disk report.
	if s.reconciler != nil {
		go func(wID string) {
			_ = s.reconciler.SyncWorker(context.Background(), wID)
		}(req.WorkerID)
	}

	httpx.WriteJSON(w, http.StatusOK, api.RegisterResponse{
		HeartbeatIntervalMS: s.cfg.HeartbeatInterval.Milliseconds(),
	})
}

// handleWorkerHeartbeat receives a periodic worker-initiated availability report.
// It refreshes last-seen time/usage in the registry, restores SUSPECT/DEAD workers
// to ALIVE when they report again, and records reported quarantine IDs as CORRUPT
// in SQLite. Unknown worker -> 404, telling the worker to register again.
// Heartbeats report state; they do not transfer chunk contents or scan the disk.
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

	// Quarantine knowledge reaches the controller here, normally on the next
	// heartbeat after a scrub detects damage. Reconciliation then evaluates repair.
	if len(req.CorruptChunks) > 0 {
		nowMs := time.Now().UnixMilli()
		for _, chID := range req.CorruptChunks {
			if err := s.store.SetReplicaState(r.Context(), chID, req.WorkerID, "CORRUPT", nowMs); err != nil {
				httpx.WriteError(w, r, api.CodeInternal, "failed to record corrupt replica")
				return
			}
		}
		if s.reconciler != nil {
			s.reconciler.Wake()
		}
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{})
}

// handleHealthz returns a small 200/ok response for Docker's liveness check.
// It proves this HTTP process can respond; it does not query worker health or
// verify that every object is readable. Use status for a cluster overview.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, api.Healthz{Status: "ok"})
}
