package datapath

import (
	"errors"
	"log/slog"
	"sync"

	"objstore/internal/controller"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

var (
	errInsufficientReplicas = errors.New("insufficient replicas achieved")
	errObjectTooLarge       = errors.New("object too large")
	errChunkUnavailable     = errors.New("chunk unavailable on all replicas")
)

// DataPath implements controller.DataPath for streaming chunk upload and verified download.
type DataPath struct {
	cfg             controller.Config
	store           metadata.Store
	registry        *registry.Registry
	placer          *placement.Placer
	client          workerclient.Client
	log             *slog.Logger
	uploadSem       chan struct{}
	bufPool         sync.Pool
	inFlight        sync.Map // objectID -> struct{}
	onReconcileWake func()
}

// New creates a new DataPath instance.
func New(
	cfg controller.Config,
	store metadata.Store,
	reg *registry.Registry,
	placer *placement.Placer,
	client workerclient.Client,
	log *slog.Logger,
) *DataPath {
	if log == nil {
		log = slog.Default()
	}
	dp := &DataPath{
		cfg:       cfg,
		store:     store,
		registry:  reg,
		placer:    placer,
		client:    client,
		log:       log,
		uploadSem: make(chan struct{}, cfg.MaxConcurrentUploads),
	}
	dp.bufPool = sync.Pool{
		New: func() any {
			b := make([]byte, cfg.ChunkSize)
			return &b
		},
	}
	return dp
}

// SetReconcileWakeHook registers a callback to wake the reconciler on corrupt chunk detection.
func (d *DataPath) SetReconcileWakeHook(hook func()) {
	d.onReconcileWake = hook
}

// IsInFlight returns true if the object is currently in the middle of being uploaded.
func (d *DataPath) IsInFlight(objectID string) bool {
	_, ok := d.inFlight.Load(objectID)
	return ok
}
