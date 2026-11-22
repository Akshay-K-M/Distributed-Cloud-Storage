// These integration tests exercise controller behavior against worker services.
package controller_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"objstore/internal/common/httpx"
	"objstore/internal/controller"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/worker"
	"objstore/internal/worker/chunkstore"
)

func TestController_RealWorkerRegistrationAndHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup Controller
	ctrlDBPath := filepath.Join(t.TempDir(), "ctrl.db")
	store, err := metadata.Open(ctrlDBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cfg := controller.Config{
		ClusterToken:         "dev-secret-change-me",
		ListenAddr:           ":8080",
		DBPath:               ctrlDBPath,
		ChunkSize:            1048576,
		ReplicationFactor:    2,
		WriteAcks:            2,
		UploadParallelism:    4,
		MaxConcurrentUploads: 16,
		MaxObjectBytes:       5368709120,
		HeartbeatInterval:    50 * time.Millisecond,
		SuspectAfter:         1 * time.Second,
		DeadAfter:            3 * time.Second,
		RepairConcurrency:    2,
	}

	reg := registry.New(store, cfg.SuspectAfter, cfg.DeadAfter, nil, nil)
	placer := placement.New(reg)
	ctrlSrv := controller.NewServer(cfg, store, reg, nil, nil)

	ctrlHTTPSrv := httptest.NewServer(ctrlSrv.Handler())
	defer ctrlHTTPSrv.Close()

	// 2. Set up a real worker node for the integration flow.
	workerDir := t.TempDir()
	workerStore, err := chunkstore.Open(workerDir)
	if err != nil {
		t.Fatal(err)
	}

	workerCfg := worker.Config{
		WorkerID:          "worker1",
		WorkerAddr:        "http://worker1:9000",
		Listen:            ":9000",
		ControllerURL:     ctrlHTTPSrv.URL,
		Token:             cfg.ClusterToken,
		DataDir:           workerDir,
		HeartbeatInterval: 50 * time.Millisecond,
		ScrubInterval:     10 * time.Minute,
		ScrubRateMBPS:     50,
		ChunkSize:         1048576,
	}

	httpClient := httpx.NewHTTPClient()
	registrar := worker.NewRegistrar(workerCfg, workerStore, httpClient, slog.Default())

	// Start real worker registrar background loop
	go registrar.Run(ctx)

	// 3. Poll controller registry until worker1 is ALIVE
	deadline := time.Now().Add(3 * time.Second)
	registered := false
	for time.Now().Before(deadline) {
		if info, ok := reg.Get("worker1"); ok && info.Status == "ALIVE" {
			registered = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !registered {
		t.Fatalf("timed out waiting for worker1 to register and heartbeat with controller")
	}

	// 4. Verify placed chunk capability
	picked, err := placer.Pick(1, nil, 1000)
	if err != nil {
		t.Fatalf("placer.Pick failed: %v", err)
	}
	if len(picked) != 1 || picked[0].ID != "worker1" {
		t.Errorf("expected worker1 to be picked, got: %+v", picked)
	}
}
