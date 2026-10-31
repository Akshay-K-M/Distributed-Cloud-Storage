// These tests cover controller routing, authentication, and metadata endpoints.
package controller_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/controller"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
)

func setupTestServer(t *testing.T) (*controller.Server, metadata.Store, *registry.Registry, controller.Config) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "server_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	cfg := controller.Config{
		ClusterToken:         "test-secret",
		ListenAddr:           ":8080",
		DBPath:               dbPath,
		ChunkSize:            1048576,
		ReplicationFactor:    2,
		WriteAcks:            2,
		UploadParallelism:    4,
		MaxConcurrentUploads: 16,
		MaxObjectBytes:       5368709120,
		DownloadPrefetch:     2,
		HeartbeatInterval:    2 * time.Second,
		SuspectAfter:         6 * time.Second,
		DeadAfter:            15 * time.Second,
		RepairConcurrency:    2,
	}

	reg := registry.New(store, cfg.SuspectAfter, cfg.DeadAfter, nil, nil)
	placer := placement.New(reg)
	srv := controller.NewServer(cfg, store, reg, placer, nil, nil)
	return srv, store, reg, cfg
}

func TestServer_Healthz(t *testing.T) {
	srv, _, _, _ := setupTestServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body api.Healthz
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "ok" {
		t.Errorf("expected ok status, got %s", body.Status)
	}
}

func TestServer_StubsReturn501(t *testing.T) {
	srv, _, _, _ := setupTestServer(t)
	handler := srv.Handler()

	// A server without a data path reports a clear configuration error.
	reqUpload := httptest.NewRequest(http.MethodPost, "/api/v1/objects", nil)
	recUpload := httptest.NewRecorder()
	handler.ServeHTTP(recUpload, reqUpload)
	if recUpload.Code != http.StatusNotImplemented {
		t.Errorf("expected 501 for upload, got %d", recUpload.Code)
	}

	reqDownload := httptest.NewRequest(http.MethodGet, "/api/v1/objects/some-id", nil)
	recDownload := httptest.NewRecorder()
	handler.ServeHTTP(recDownload, reqDownload)
	if recDownload.Code != http.StatusNotImplemented {
		t.Errorf("expected 501 for download, got %d", recDownload.Code)
	}
}

func TestServer_InternalBearerAuth(t *testing.T) {
	srv, _, _, cfg := setupTestServer(t)
	handler := srv.Handler()

	// Missing token -> 401
	req := httptest.NewRequest(http.MethodPost, "/internal/workers/register", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing token, got %d", rec.Code)
	}

	// Invalid token -> 401
	reqBad := httptest.NewRequest(http.MethodPost, "/internal/workers/register", strings.NewReader(`{}`))
	reqBad.Header.Set("Authorization", "Bearer wrong-token")
	recBad := httptest.NewRecorder()
	handler.ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for bad token, got %d", recBad.Code)
	}

	// Correct token
	regReq := api.RegisterRequest{
		WorkerID:      "worker1",
		Address:       "http://worker1:9000",
		CapacityBytes: 1000000,
		UsedBytes:     100,
		ChunkCount:    1,
	}
	body, _ := json.Marshal(regReq)
	reqOK := httptest.NewRequest(http.MethodPost, "/internal/workers/register", bytes.NewReader(body))
	reqOK.Header.Set("Authorization", "Bearer "+cfg.ClusterToken)
	recOK := httptest.NewRecorder()
	handler.ServeHTTP(recOK, reqOK)
	if recOK.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid register, got %d: %s", recOK.Code, recOK.Body.String())
	}
}

func TestServer_RegisterAndHeartbeatFlow(t *testing.T) {
	srv, _, _, cfg := setupTestServer(t)
	handler := srv.Handler()

	// 1. Heartbeat before register -> 404 UNKNOWN_WORKER
	hbReq := api.HeartbeatRequest{
		WorkerID:      "worker1",
		UsedBytes:     200,
		CapacityBytes: 1000000,
		ChunkCount:    2,
	}
	hbBytes, _ := json.Marshal(hbReq)
	req1 := httptest.NewRequest(http.MethodPost, "/internal/workers/heartbeat", bytes.NewReader(hbBytes))
	req1.Header.Set("Authorization", "Bearer "+cfg.ClusterToken)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown worker, got %d", rec1.Code)
	}
	var errBody api.ErrorBody
	_ = json.NewDecoder(rec1.Body).Decode(&errBody)
	if errBody.Error.Code != api.CodeUnknownWorker {
		t.Errorf("expected UNKNOWN_WORKER error code, got %s", errBody.Error.Code)
	}

	// 2. Register worker1
	regReq := api.RegisterRequest{
		WorkerID:      "worker1",
		Address:       "http://worker1:9000",
		CapacityBytes: 1000000,
		UsedBytes:     100,
		ChunkCount:    1,
	}
	regBytes, _ := json.Marshal(regReq)
	req2 := httptest.NewRequest(http.MethodPost, "/internal/workers/register", bytes.NewReader(regBytes))
	req2.Header.Set("Authorization", "Bearer "+cfg.ClusterToken)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("register failed with %d: %s", rec2.Code, rec2.Body.String())
	}

	// 3. Heartbeat succeeds
	req3 := httptest.NewRequest(http.MethodPost, "/internal/workers/heartbeat", bytes.NewReader(hbBytes))
	req3.Header.Set("Authorization", "Bearer "+cfg.ClusterToken)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("heartbeat failed with %d: %s", rec3.Code, rec3.Body.String())
	}

	// 4. Admin status includes worker1
	reqAdmin := httptest.NewRequest(http.MethodGet, "/api/v1/admin/status", nil)
	recAdmin := httptest.NewRecorder()
	handler.ServeHTTP(recAdmin, reqAdmin)
	if recAdmin.Code != http.StatusOK {
		t.Fatalf("admin status returned %d", recAdmin.Code)
	}
	var status api.AdminStatus
	_ = json.NewDecoder(recAdmin.Body).Decode(&status)
	if len(status.Workers) != 1 || status.Workers[0].WorkerID != "worker1" {
		t.Errorf("expected worker1 in admin status, got %+v", status.Workers)
	}
	if status.Workers[0].Status != "ALIVE" {
		t.Errorf("expected worker1 to be ALIVE, got %s", status.Workers[0].Status)
	}
}

func TestServer_MetadataEndpoints(t *testing.T) {
	srv, store, _, _ := setupTestServer(t)
	handler := srv.Handler()
	ctx := context.Background()

	// Seed an object
	objID, err := store.BeginUpload(ctx, "photos/landscape.jpg", "image/jpeg", false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CommitObject(ctx, metadata.CommitInput{
		ObjectID:       objID,
		Name:           "photos/landscape.jpg",
		Size:           1048576,
		ChecksumSHA256: "sha256landscape",
		ChunkCount:     1,
		ContentType:    "image/jpeg",
		CustomMetadata: map[string]string{"camera": "dslr"},
		Chunks: []metadata.ChunkCommit{
			{ChunkID: objID + "_0", Index: 0, Size: 1048576, SHA256: "sha256landscape", Replicas: []string{"w1", "w2"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. GET /api/v1/objects/{id}/metadata
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+objID+"/metadata", nil)
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recGet.Code, recGet.Body.String())
	}
	var detail api.ObjectDetail
	_ = json.NewDecoder(recGet.Body).Decode(&detail)
	if detail.Name != "photos/landscape.jpg" || len(detail.Chunks) != 1 {
		t.Errorf("unexpected detail: %+v", detail)
	}

	// 2. PATCH /api/v1/objects/{id}/metadata
	patchBody := `{"location": "yosemite", "camera": null}`
	reqPatch := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+objID+"/metadata", strings.NewReader(patchBody))
	recPatch := httptest.NewRecorder()
	handler.ServeHTTP(recPatch, reqPatch)
	if recPatch.Code != http.StatusOK {
		t.Fatalf("expected 200 on patch, got %d: %s", recPatch.Code, recPatch.Body.String())
	}
	var patchedDetail api.ObjectDetail
	_ = json.NewDecoder(recPatch.Body).Decode(&patchedDetail)
	if patchedDetail.CustomMetadata["location"] != "yosemite" {
		t.Errorf("expected location=yosemite, got %s", patchedDetail.CustomMetadata["location"])
	}
	if _, ok := patchedDetail.CustomMetadata["camera"]; ok {
		t.Errorf("expected camera key to be removed")
	}

	// 3. PATCH with non-string value -> 400
	reqBadPatch := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+objID+"/metadata", strings.NewReader(`{"num": 123}`))
	recBadPatch := httptest.NewRecorder()
	handler.ServeHTTP(recBadPatch, reqBadPatch)
	if recBadPatch.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on non-string patch, got %d", recBadPatch.Code)
	}

	// 4. GET /api/v1/objects listing
	reqList := httptest.NewRequest(http.MethodGet, "/api/v1/objects?prefix=photos/", nil)
	recList := httptest.NewRecorder()
	handler.ServeHTTP(recList, reqList)
	if recList.Code != http.StatusOK {
		t.Fatalf("expected 200 on list, got %d", recList.Code)
	}
	var listResp api.ListResponse
	_ = json.NewDecoder(recList.Body).Decode(&listResp)
	if len(listResp.Objects) != 1 || listResp.Objects[0].Name != "photos/landscape.jpg" {
		t.Errorf("unexpected list response: %+v", listResp)
	}

	// 5. DELETE /api/v1/objects/{id} -> 204
	reqDel := httptest.NewRequest(http.MethodDelete, "/api/v1/objects/"+objID, nil)
	recDel := httptest.NewRecorder()
	handler.ServeHTTP(recDel, reqDel)
	if recDel.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete, got %d", recDel.Code)
	}

	// 6. GET metadata after delete -> 404
	recGetAfterDel := httptest.NewRecorder()
	handler.ServeHTTP(recGetAfterDel, reqGet)
	if recGetAfterDel.Code != http.StatusNotFound {
		t.Errorf("expected 404 for deleted object metadata, got %d", recGetAfterDel.Code)
	}
}
