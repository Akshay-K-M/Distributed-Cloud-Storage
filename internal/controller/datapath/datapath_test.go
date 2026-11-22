// These tests cover streaming upload and download behavior at the controller boundary.
package datapath_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/controller"
	"objstore/internal/controller/datapath"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

type testHarness struct {
	cfg     controller.Config
	store   metadata.Store
	reg     *registry.Registry
	placer  *placement.Placer
	fake    *workerclient.FakeClient
	dp      *datapath.DataPath
	server  *controller.Server
	handler http.Handler
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	cfg := controller.Config{
		ClusterToken:         "test-token",
		ListenAddr:           ":8080",
		DBPath:               dbPath,
		ChunkSize:            1048576, // 1 MiB
		ReplicationFactor:    2,
		WriteAcks:            2,
		UploadParallelism:    4,
		MaxConcurrentUploads: 16,
		MaxObjectBytes:       5368709120,
		SuspectAfter:         6 * time.Second,
		DeadAfter:            15 * time.Second,
		RepairEnabled:        true,
		RepairConcurrency:    2,
	}

	reg := registry.New(store, cfg.SuspectAfter, cfg.DeadAfter, nil, nil)
	placer := placement.New(reg)
	fake := workerclient.NewFake()
	dp := datapath.New(cfg, store, reg, placer, fake, nil)
	ctrlServer := controller.NewServer(cfg, store, reg, dp, nil)

	ctx := context.Background()
	// Register 4 alive workers
	for i := 1; i <= 4; i++ {
		wID := fmt.Sprintf("worker%d", i)
		err := reg.Register(ctx, api.RegisterRequest{
			WorkerID:      wID,
			Address:       fmt.Sprintf("http://%s:9000", wID),
			CapacityBytes: 100 * 1024 * 1024,
			UsedBytes:     0,
			ChunkCount:    0,
		})
		if err != nil {
			t.Fatalf("register worker failed: %v", err)
		}
	}

	return &testHarness{
		cfg:     cfg,
		store:   store,
		reg:     reg,
		placer:  placer,
		fake:    fake,
		dp:      dp,
		server:  ctrlServer,
		handler: ctrlServer.Handler(),
	}
}

func makeMultipartRequest(t *testing.T, url string, name string, fileData io.Reader, filename string, contentType string, meta map[string]string) *http.Request {
	t.Helper()
	body := &bytes.Buffer{}
	mpw := multipart.NewWriter(body)

	if name != "" {
		_ = mpw.WriteField("name", name)
	}
	if meta != nil {
		b, _ := json.Marshal(meta)
		_ = mpw.WriteField("metadata", string(b))
	}
	if fileData != nil {
		h := make(textproto.MIMEHeader)
		if filename != "" {
			h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
		} else {
			h.Set("Content-Disposition", `form-data; name="file"`)
		}
		if contentType != "" {
			h.Set("Content-Type", contentType)
		}
		pw, _ := mpw.CreatePart(h)
		_, _ = io.Copy(pw, fileData)
	}

	_ = mpw.Close()
	req := httptest.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", mpw.FormDataContentType())
	return req
}

// TestSizesRoundTrip tests: 0, 1, 1048575, 1048576, 1048577, 3.5 MiB
func TestSizesRoundTrip(t *testing.T) {
	h := newTestHarness(t)

	sizes := []int{0, 1, 1048575, 1048576, 1048577, int(3.5 * 1024 * 1024)}

	for _, sz := range sizes {
		t.Run(fmt.Sprintf("Size_%d", sz), func(t *testing.T) {
			data := make([]byte, sz)
			if sz > 0 {
				_, _ = rand.Read(data)
			}
			dataSHA := sha256.Sum256(data)
			wantSHA := hex.EncodeToString(dataSHA[:])

			objName := fmt.Sprintf("test-size-%d.bin", sz)
			req := makeMultipartRequest(t, "/api/v1/objects", objName, bytes.NewReader(data), objName, "application/octet-stream", nil)
			rec := httptest.NewRecorder()
			h.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusCreated {
				t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
			}

			var created api.Object
			if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
				t.Fatalf("unmarshal created object: %v", err)
			}

			if created.Size != int64(sz) {
				t.Errorf("size mismatch: got %d, want %d", created.Size, sz)
			}
			if created.ChecksumSHA256 != wantSHA {
				t.Errorf("sha mismatch: got %s, want %s", created.ChecksumSHA256, wantSHA)
			}

			// Verify chunk count
			expectedChunks := (sz + int(h.cfg.ChunkSize) - 1) / int(h.cfg.ChunkSize)
			if sz == 0 {
				expectedChunks = 1 // Zero-byte object has 1 empty chunk
			}
			if created.ChunkCount != expectedChunks {
				t.Errorf("chunk count mismatch: got %d, want %d", created.ChunkCount, expectedChunks)
			}

			// Download object and verify SHA
			dlReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+created.ObjectID, nil)
			dlRec := httptest.NewRecorder()
			h.handler.ServeHTTP(dlRec, dlReq)

			if dlRec.Code != http.StatusOK {
				t.Fatalf("download failed with %d: %s", dlRec.Code, dlRec.Body.String())
			}

			dlBytes := dlRec.Body.Bytes()
			if len(dlBytes) != sz {
				t.Fatalf("downloaded bytes mismatch: got %d, want %d", len(dlBytes), sz)
			}
			dlSHA := sha256.Sum256(dlBytes)
			if hex.EncodeToString(dlSHA[:]) != wantSHA {
				t.Fatalf("downloaded sha mismatch")
			}

			// Verify metadata detail
			detail, err := h.store.GetObjectWithChunks(context.Background(), created.ObjectID)
			if err != nil {
				t.Fatalf("GetObjectWithChunks failed: %v", err)
			}
			lastChunk := detail.Chunks[len(detail.Chunks)-1]
			expectedLastSize := int64(sz % int(h.cfg.ChunkSize))
			if expectedLastSize == 0 && sz > 0 {
				expectedLastSize = h.cfg.ChunkSize
			}
			if lastChunk.Size != expectedLastSize {
				t.Errorf("last chunk size mismatch: got %d, want %d", lastChunk.Size, expectedLastSize)
			}
		})
	}
}

// TestReplicationAndDistribution tests: Replicas always on 2 distinct workers; distribution roughly even over 40 uploads.
func TestReplicationAndDistribution(t *testing.T) {
	h := newTestHarness(t)

	// Perform 40 uploads of 1 MiB each
	const numUploads = 40
	for i := 0; i < numUploads; i++ {
		data := make([]byte, 1048576)
		objName := fmt.Sprintf("dist-%d.bin", i)
		req := makeMultipartRequest(t, "/api/v1/objects", objName, bytes.NewReader(data), objName, "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("upload %d failed: %d: %s", i, rec.Code, rec.Body.String())
		}

		var obj api.Object
		_ = json.Unmarshal(rec.Body.Bytes(), &obj)

		detail, err := h.store.GetObjectWithChunks(context.Background(), obj.ObjectID)
		if err != nil {
			t.Fatalf("get detail failed: %v", err)
		}

		for _, chunk := range detail.Chunks {
			if len(chunk.Replicas) != 2 {
				t.Fatalf("chunk %s expected 2 replicas, got %d", chunk.ChunkID, len(chunk.Replicas))
			}
			if chunk.Replicas[0].WorkerID == chunk.Replicas[1].WorkerID {
				t.Fatalf("chunk %s has duplicate replica worker: %s", chunk.ChunkID, chunk.Replicas[0].WorkerID)
			}
		}
	}

	// Assert distribution across 4 workers: total replicas = 40 * 2 = 80 replicas, roughly 20 per worker
	workers := h.reg.Alive()
	if len(workers) != 4 {
		t.Fatalf("expected 4 alive workers, got %d", len(workers))
	}

	var minUsed, maxUsed int64 = 1 << 60, -1
	for _, w := range workers {
		if w.UsedBytes < minUsed {
			minUsed = w.UsedBytes
		}
		if w.UsedBytes > maxUsed {
			maxUsed = w.UsedBytes
		}
	}

	// Difference between max and min used bytes should be within one chunk-ish tolerance (e.g. <= 2 chunks = 2 MiB)
	diff := maxUsed - minUsed
	if diff > 2*h.cfg.ChunkSize {
		t.Errorf("worker distribution skew too large: maxUsed=%d, minUsed=%d, diff=%d", maxUsed, minUsed, diff)
	}
}

// TestUploadFailureInjection tests retry on single worker failure and 503 on double failure.
func TestUploadFailureInjection(t *testing.T) {
	t.Run("SingleWorkerFailureRetries", func(t *testing.T) {
		h := newTestHarness(t)

		// worker1 fails all PUTs
		h.fake.BeforePut = func(w workerclient.WorkerRef, chunkID string) error {
			if w.ID == "worker1" {
				return workerclient.ErrUnavailable
			}
			return nil
		}

		data := make([]byte, 1024)
		req := makeMultipartRequest(t, "/api/v1/objects", "retry.bin", bytes.NewReader(data), "retry.bin", "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created despite worker1 failure, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("TwoWorkersFailReturns503AndNoObjectVisible", func(t *testing.T) {
		h := newTestHarness(t)

		// worker1, worker2, worker3 fail all PUTs => impossible to get 2 replicas
		h.fake.BeforePut = func(w workerclient.WorkerRef, chunkID string) error {
			if w.ID == "worker1" || w.ID == "worker2" || w.ID == "worker3" {
				return workerclient.ErrUnavailable
			}
			return nil
		}

		data := make([]byte, 1024)
		req := makeMultipartRequest(t, "/api/v1/objects", "fail.bin", bytes.NewReader(data), "fail.bin", "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
		}

		// Ensure no object is visible
		listRes, err := h.store.List(context.Background(), metadata.ListParams{Prefix: ""})
		if err != nil {
			t.Fatalf("list failed: %v", err)
		}
		if len(listRes.Objects) != 0 {
			t.Errorf("expected 0 objects, got %d", len(listRes.Objects))
		}
	})

	t.Run("FewerThan2WorkersReturns503", func(t *testing.T) {
		h := newTestHarness(t)

		// Mark 3 workers DEAD
		for i := 2; i <= 4; i++ {
			_ = h.store.SetWorkerStatus(context.Background(), fmt.Sprintf("worker%d", i), "DEAD", time.Now().UnixMilli())
		}
		// Trigger sweep or reload to update in-memory registry
		_ = h.reg.LoadFromDB(context.Background())
		// Sweep with long time to make sure they are DEAD in memory
		h.reg.Sweep(context.Background())

		data := make([]byte, 1024)
		req := makeMultipartRequest(t, "/api/v1/objects", "nofit.bin", bytes.NewReader(data), "nofit.bin", "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("ChecksumMismatchPUTTreatedAsFailure", func(t *testing.T) {
		h := newTestHarness(t)

		h.fake.BeforePut = func(w workerclient.WorkerRef, chunkID string) error {
			if w.ID == "worker1" {
				return workerclient.ErrChecksumMismatch
			}
			return nil
		}

		data := make([]byte, 1024)
		req := makeMultipartRequest(t, "/api/v1/objects", "csmismatch.bin", bytes.NewReader(data), "csmismatch.bin", "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created via retry, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

// TestDownloadCorruptionAndFallback tests read verification and fallback.
func TestDownloadCorruptionAndFallback(t *testing.T) {
	t.Run("CorruptedReplicaServedFromOtherAndMarkedCORRUPT", func(t *testing.T) {
		h := newTestHarness(t)

		data := []byte("hello distributed world")
		req := makeMultipartRequest(t, "/api/v1/objects", "corrupt-test.txt", bytes.NewReader(data), "corrupt-test.txt", "text/plain", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		var obj api.Object
		_ = json.Unmarshal(rec.Body.Bytes(), &obj)

		_, err := h.store.GetObjectWithChunks(context.Background(), obj.ObjectID)
		if err != nil {
			t.Fatalf("GetObjectWithChunks: %v", err)
		}

		// Corrupt whichever replica is attempted first
		var firstWorker string
		var mu sync.Mutex
		h.fake.CorruptGet = func(w workerclient.WorkerRef, chunkID string, data []byte) []byte {
			mu.Lock()
			defer mu.Unlock()
			if firstWorker == "" {
				firstWorker = w.ID
				corrupted := append([]byte(nil), data...)
				if len(corrupted) > 0 {
					corrupted[0] ^= 0xFF
				}
				return corrupted
			}
			return data
		}

		dlReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+obj.ObjectID, nil)
		dlRec := httptest.NewRecorder()
		h.handler.ServeHTTP(dlRec, dlReq)

		if dlRec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK via fallback, got %d: %s", dlRec.Code, dlRec.Body.String())
		}

		if dlRec.Body.String() != string(data) {
			t.Fatalf("downloaded data mismatch: got %q, want %q", dlRec.Body.String(), string(data))
		}

		if firstWorker == "" {
			t.Fatalf("expected at least one worker to be called")
		}

		// Check that the first tried worker replica is marked CORRUPT in SQLite
		updatedDetail, err := h.store.GetObjectWithChunks(context.Background(), obj.ObjectID)
		if err != nil {
			t.Fatalf("get updated detail: %v", err)
		}
		for _, rep := range updatedDetail.Chunks[0].Replicas {
			if rep.WorkerID == firstWorker {
				if rep.State != "CORRUPT" {
					t.Errorf("expected worker %s replica state to be CORRUPT, got %s", firstWorker, rep.State)
				}
			}
		}
	})

	t.Run("BothReplicasCorruptOnFinalChunkReturnsJSON503", func(t *testing.T) {
		h := newTestHarness(t)

		data := bytes.Repeat([]byte("x"), (1<<20)+1)
		req := makeMultipartRequest(t, "/api/v1/objects", "both-corrupt.txt", bytes.NewReader(data), "both-corrupt.txt", "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		var obj api.Object
		_ = json.Unmarshal(rec.Body.Bytes(), &obj)

		// Leave the first chunk readable and damage both copies of the final chunk.
		h.fake.CorruptGet = func(w workerclient.WorkerRef, chunkID string, data []byte) []byte {
			if chunkID != obj.ObjectID+"_1" {
				return data
			}
			corrupted := append([]byte(nil), data...)
			if len(corrupted) > 0 {
				corrupted[0] ^= 0xFF
			}
			return corrupted
		}

		dlReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+obj.ObjectID, nil)
		dlRec := httptest.NewRecorder()
		h.handler.ServeHTTP(dlRec, dlReq)

		if dlRec.Code != http.StatusServiceUnavailable || !json.Valid(dlRec.Body.Bytes()) {
			t.Fatalf("expected 503, got %d: %s", dlRec.Code, dlRec.Body.String())
		}
	})
}

// TestConcurrencyAndRace runs 16 parallel uploads and 16 parallel downloads.
func TestConcurrencyAndRace(t *testing.T) {
	h := newTestHarness(t)

	var wg sync.WaitGroup
	objIDs := make([]string, 16)
	var mu sync.Mutex

	// 16 parallel uploads
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("concurrency test data payload %d", idx))
			objName := fmt.Sprintf("conc-%d.txt", idx)
			req := makeMultipartRequest(t, "/api/v1/objects", objName, bytes.NewReader(data), objName, "text/plain", nil)
			rec := httptest.NewRecorder()
			h.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusCreated {
				t.Errorf("parallel upload %d failed with code %d", idx, rec.Code)
				return
			}
			var obj api.Object
			_ = json.Unmarshal(rec.Body.Bytes(), &obj)
			mu.Lock()
			objIDs[idx] = obj.ObjectID
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// 16 parallel downloads
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mu.Lock()
			id := objIDs[idx]
			mu.Unlock()
			if id == "" {
				return
			}

			req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+id, nil)
			rec := httptest.NewRecorder()
			h.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Errorf("parallel download %d failed with code %d", idx, rec.Code)
			}
		}(i)
	}
	wg.Wait()
}

// TestNameConflictAndOverwrite tests 409 and overwrite=true.
func TestNameConflictAndOverwrite(t *testing.T) {
	h := newTestHarness(t)

	data1 := []byte("original content")
	req1 := makeMultipartRequest(t, "/api/v1/objects", "conflict.txt", bytes.NewReader(data1), "conflict.txt", "", nil)
	rec1 := httptest.NewRecorder()
	h.handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("upload 1 failed: %d", rec1.Code)
	}

	var obj1 api.Object
	_ = json.Unmarshal(rec1.Body.Bytes(), &obj1)

	// Second upload with same name without overwrite -> 409
	data2 := []byte("new content")
	req2 := makeMultipartRequest(t, "/api/v1/objects", "conflict.txt", bytes.NewReader(data2), "conflict.txt", "", nil)
	rec2 := httptest.NewRecorder()
	h.handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d: %s", rec2.Code, rec2.Body.String())
	}

	// Third upload with same name and overwrite=true -> 201
	req3 := makeMultipartRequest(t, "/api/v1/objects?overwrite=true", "conflict.txt", bytes.NewReader(data2), "conflict.txt", "", nil)
	rec3 := httptest.NewRecorder()
	h.handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on overwrite, got %d: %s", rec3.Code, rec3.Body.String())
	}

	var obj3 api.Object
	_ = json.Unmarshal(rec3.Body.Bytes(), &obj3)

	// Old object should now return 404
	getOldReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+obj1.ObjectID, nil)
	getOldRec := httptest.NewRecorder()
	h.handler.ServeHTTP(getOldRec, getOldReq)
	if getOldRec.Code != http.StatusNotFound {
		t.Errorf("expected old object to return 404, got %d", getOldRec.Code)
	}

	// New object should return 200 with new data
	getNewReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+obj3.ObjectID, nil)
	getNewRec := httptest.NewRecorder()
	h.handler.ServeHTTP(getNewRec, getNewReq)
	if getNewRec.Code != http.StatusOK || getNewRec.Body.String() != string(data2) {
		t.Errorf("expected new object content, got code %d body %q", getNewRec.Code, getNewRec.Body.String())
	}
}

// TestMultipartEdgeCases tests: metadata after file, missing file, bad JSON metadata, name with slashes, name with leading slash (400).
func TestMultipartEdgeCases(t *testing.T) {
	h := newTestHarness(t)

	t.Run("UnnamedFile", func(t *testing.T) {
		body := new(bytes.Buffer)
		form := multipart.NewWriter(body)
		if err := form.WriteField("file", "unnamed data"); err != nil {
			t.Fatal(err)
		}
		if err := form.Close(); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", form.FormDataContentType())
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)
		var obj api.Object
		if rec.Code != 201 || json.Unmarshal(rec.Body.Bytes(), &obj) != nil || obj.Name == "" {
			t.Fatalf("unnamed upload: %d %s", rec.Code, rec.Body)
		}
	})

	for _, invalid := range []string{"null", `{"owner":null}`} {
		t.Run("Invalid metadata "+invalid, func(t *testing.T) {
			body := new(bytes.Buffer)
			form := multipart.NewWriter(body)
			if err := form.WriteField("metadata", invalid); err != nil {
				t.Fatal(err)
			}
			if err := form.WriteField("file", "some bytes"); err != nil {
				t.Fatal(err)
			}
			if err := form.Close(); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
			req.Header.Set("Content-Type", form.FormDataContentType())
			rec := httptest.NewRecorder()
			h.handler.ServeHTTP(rec, req)
			if rec.Code != 400 {
				t.Fatalf("got %d, want 400 for %s", rec.Code, invalid)
			}
		})
	}

	t.Run("InvalidMetadataAfterFileRollsBack", func(t *testing.T) {
		body := new(bytes.Buffer)
		form := multipart.NewWriter(body)
		file, err := form.CreateFormFile("file", "rollback.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, "rollback"); err != nil {
			t.Fatal(err)
		}
		if err := form.WriteField("metadata", "invalid"); err != nil {
			t.Fatal(err)
		}
		if err := form.Close(); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", form.FormDataContentType())
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)
		counts, err := h.store.ObjectCounts(context.Background())
		if err != nil || rec.Code != 400 || counts.Uploading != 0 {
			t.Fatalf("invalid upload: status=%d counts=%+v err=%v", rec.Code, counts, err)
		}
	})

	t.Run("MetadataAfterFile", func(t *testing.T) {
		body := &bytes.Buffer{}
		mpw := multipart.NewWriter(body)

		// 1. file part first
		hPart := make(textproto.MIMEHeader)
		hPart.Set("Content-Disposition", `form-data; name="file"; filename="meta_after.txt"`)
		pw, _ := mpw.CreatePart(hPart)
		_, _ = pw.Write([]byte("data with metadata after"))

		// 2. metadata part after
		_ = mpw.WriteField("metadata", `{"custom_key":"custom_val"}`)
		_ = mpw.Close()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", mpw.FormDataContentType())
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created for metadata after file, got %d: %s", rec.Code, rec.Body.String())
		}

		var obj api.Object
		_ = json.Unmarshal(rec.Body.Bytes(), &obj)
		if obj.CustomMetadata["custom_key"] != "custom_val" {
			t.Errorf("expected custom_metadata key to be preserved: %+v", obj.CustomMetadata)
		}
	})

	t.Run("MissingFile", func(t *testing.T) {
		body := &bytes.Buffer{}
		mpw := multipart.NewWriter(body)
		_ = mpw.WriteField("name", "nofile.txt")
		_ = mpw.Close()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", mpw.FormDataContentType())
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for missing file, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("BadJSONMetadata", func(t *testing.T) {
		body := &bytes.Buffer{}
		mpw := multipart.NewWriter(body)
		_ = mpw.WriteField("name", "badmeta.txt")
		_ = mpw.WriteField("metadata", `{invalid-json}`)
		hPart := make(textproto.MIMEHeader)
		hPart.Set("Content-Disposition", `form-data; name="file"; filename="badmeta.txt"`)
		pw, _ := mpw.CreatePart(hPart)
		_, _ = pw.Write([]byte("data"))
		_ = mpw.Close()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", mpw.FormDataContentType())
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for bad JSON metadata, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("NameWithLeadingSlash", func(t *testing.T) {
		req := makeMultipartRequest(t, "/api/v1/objects", "/leading/slash.txt", bytes.NewReader([]byte("x")), "slash.txt", "", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for leading slash in name, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("NameWithSlashesValid", func(t *testing.T) {
		req := makeMultipartRequest(t, "/api/v1/objects", "photos/2026/vacation/cat.jpg", bytes.NewReader([]byte("cat photo")), "cat.jpg", "image/jpeg", nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201 for valid name with internal slashes, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

// TestBoundedMemory asserts controller memory stays bounded during large synthetic upload.
func TestBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 200 MiB memory test in short mode")
	}

	h := newTestHarness(t)
	// Replace fake client with a discarding client so worker in-memory store doesn't retain 400 MiB
	discardClient := &discardWorkerClient{}
	dp := datapath.New(h.cfg, h.store, h.reg, h.placer, discardClient, nil)
	ctrlServer := controller.NewServer(h.cfg, h.store, h.reg, dp, nil)
	handler := ctrlServer.Handler()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Stream 200 MiB synthetic stream
	totalSize := int64(200 * 1024 * 1024)
	zeroReader := io.LimitReader(zeroReaderType{}, totalSize)

	req := makeMultipartRequest(t, "/api/v1/objects", "large200mb.bin", zeroReader, "large200mb.bin", "application/octet-stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("upload 200 MiB failed: %d: %s", rec.Code, rec.Body.String())
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	heapGrowth := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
	// Assert heap growth on controller stays under ~100 MiB (configured bounds: 16 x 4 x 1 MiB = 64 MiB)
	if heapGrowth > 100*1024*1024 {
		t.Errorf("heap growth exceeded 100 MiB: %d bytes", heapGrowth)
	}
}

type discardWorkerClient struct{}

func (discardWorkerClient) PutChunk(ctx context.Context, w workerclient.WorkerRef, chunkID, sha256hex string, size int64, body io.Reader) error {
	h := sha256.New()
	n, err := io.Copy(h, body)
	if err != nil {
		return err
	}
	if n != size || hex.EncodeToString(h.Sum(nil)) != sha256hex {
		return workerclient.ErrChecksumMismatch
	}
	return nil
}

func (discardWorkerClient) GetChunk(ctx context.Context, w workerclient.WorkerRef, chunkID string) (io.ReadCloser, int64, string, error) {
	return nil, 0, "", workerclient.ErrChunkNotFound
}

func (discardWorkerClient) DeleteChunk(ctx context.Context, w workerclient.WorkerRef, chunkID string) error {
	return nil
}

func (discardWorkerClient) ListChunks(ctx context.Context, w workerclient.WorkerRef) (*api.Inventory, error) {
	return &api.Inventory{WorkerID: w.ID}, nil
}

func (discardWorkerClient) Scrub(ctx context.Context, w workerclient.WorkerRef) error {
	return nil
}

type zeroReaderType struct{}

func (zeroReaderType) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
