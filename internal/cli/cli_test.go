// These tests cover CLI parsing, command behavior, and user-visible errors.
package cli_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"objstore/internal/api"
	"objstore/internal/cli"
	"objstore/internal/controller"
	"objstore/internal/controller/datapath"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/workerclient"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		input      string
		wantRemote bool
		wantID     bool
		wantValue  string
	}{
		{"obj:photos/cat.jpg", true, false, "photos/cat.jpg"},
		{"obj:/photos/cat.jpg", true, false, "photos/cat.jpg"},
		{"obj:cat.jpg", true, false, "cat.jpg"},
		{"obj:", true, false, ""},
		{"id:0190c3a2-1234", true, true, "0190c3a2-1234"},
		{"local/file.txt", false, false, "local/file.txt"},
		{"-", false, false, "-"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			isRemote, isID, val := cli.ParseTarget(tt.input)
			if isRemote != tt.wantRemote || isID != tt.wantID || val != tt.wantValue {
				t.Errorf("ParseTarget(%q) = (%v, %v, %q), want (%v, %v, %q)",
					tt.input, isRemote, isID, val, tt.wantRemote, tt.wantID, tt.wantValue)
			}
		})
	}
}

func TestUploadMultipartStructureAndOrder(t *testing.T) {
	var partOrder []string
	var receivedName string
	var receivedMeta string
	var fileContent string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/objects" && r.Method == http.MethodPost {
			mr, err := r.MultipartReader()
			if err != nil {
				t.Fatalf("MultipartReader error: %v", err)
			}
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("NextPart error: %v", err)
				}
				formName := p.FormName()
				partOrder = append(partOrder, formName)
				b, _ := io.ReadAll(p)
				switch formName {
				case "name":
					receivedName = string(b)
				case "metadata":
					receivedMeta = string(b)
				case "file":
					fileContent = string(b)
				}
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(api.Object{
				ObjectID:       "0190c3a2-0000",
				Name:           receivedName,
				Size:           int64(len(fileContent)),
				ChecksumSHA256: "testsha",
				ChunkCount:     1,
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "upload.txt")
	_ = os.WriteFile(localFile, []byte("hello multipart"), 0644)

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)
	code := app.Run([]string{
		"--endpoint", ts.URL,
		"cp", localFile, "obj:docs/upload.txt",
		"--meta", "owner=alice",
	})

	if code != 0 {
		t.Fatalf("cp upload failed with exit code %d: %s", code, stderr.String())
	}

	// Assert parts order: name and metadata BEFORE file
	if len(partOrder) < 3 {
		t.Fatalf("expected 3 parts, got %v", partOrder)
	}
	if partOrder[0] != "name" {
		t.Errorf("part 0 expected 'name', got %q", partOrder[0])
	}
	if partOrder[1] != "metadata" {
		t.Errorf("part 1 expected 'metadata', got %q", partOrder[1])
	}
	if partOrder[2] != "file" {
		t.Errorf("part 2 expected 'file', got %q", partOrder[2])
	}

	if receivedName != "docs/upload.txt" {
		t.Errorf("received name: %s", receivedName)
	}
	if !strings.Contains(receivedMeta, `"owner":"alice"`) {
		t.Errorf("received metadata: %s", receivedMeta)
	}
	if fileContent != "hello multipart" {
		t.Errorf("received file content: %s", fileContent)
	}
}

func TestDownloadAndCat(t *testing.T) {
	fileContent := "sample content to download"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/objects" && r.URL.Query().Get("name") == "sample.txt" {
			_ = json.NewEncoder(w).Encode(api.ListResponse{
				Objects: []api.ListObject{
					{ObjectID: "obj-123", Name: "sample.txt", Size: int64(len(fileContent))},
				},
			})
			return
		}
		if r.URL.Path == "/api/v1/objects/obj-123" && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileContent)))
			_, _ = w.Write([]byte(fileContent))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	// 1. Test cat obj:sample.txt
	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)
	code := app.Run([]string{"--endpoint", ts.URL, "cat", "obj:sample.txt"})
	if code != 0 {
		t.Fatalf("cat failed with %d: %s", code, stderr.String())
	}
	if stdout.String() != fileContent {
		t.Errorf("cat output mismatch: got %q, want %q", stdout.String(), fileContent)
	}

	// 2. Test cp obj:sample.txt <file>
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "downloaded.txt")
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "cp", "obj:sample.txt", destFile})
	if code != 0 {
		t.Fatalf("cp download failed with %d: %s", code, stderr.String())
	}
	data, err := os.ReadFile(destFile)
	if err != nil || string(data) != fileContent {
		t.Errorf("downloaded file mismatch: got %q, want %q, err: %v", string(data), fileContent, err)
	}
}

func TestListWithPaginationAndDelimiter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		cursor := q.Get("cursor")

		if cursor == "" {
			// Page 1
			_ = json.NewEncoder(w).Encode(api.ListResponse{
				CommonPrefixes: []string{"photos/"},
				Objects: []api.ListObject{
					{ObjectID: "id-1", Name: "a.txt", Size: 10, CreatedAt: "2026-10-21T00:00:00Z"},
				},
				Truncated:  true,
				NextCursor: "cursor-page-2",
			})
			return
		} else if cursor == "cursor-page-2" {
			// Page 2
			_ = json.NewEncoder(w).Encode(api.ListResponse{
				Objects: []api.ListObject{
					{ObjectID: "id-2", Name: "b.txt", Size: 20, CreatedAt: "2026-10-21T00:01:00Z"},
				},
				Truncated:  false,
				NextCursor: "",
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)
	code := app.Run([]string{"--endpoint", ts.URL, "ls"})
	if code != 0 {
		t.Fatalf("ls failed with %d: %s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "photos/") || !strings.Contains(out, "a.txt") || !strings.Contains(out, "b.txt") {
		t.Errorf("unexpected ls output across pages:\n%s", out)
	}

	// Test ls -l
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "ls", "-l"})
	if code != 0 {
		t.Fatalf("ls -l failed with %d: %s", code, stderr.String())
	}
	outLong := stdout.String()
	if !strings.Contains(outLong, "DIR") || !strings.Contains(outLong, "id-1") || !strings.Contains(outLong, "id-2") {
		t.Errorf("unexpected ls -l output:\n%s", outLong)
	}
}

func TestStatAndStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/objects" && r.URL.Query().Get("name") == "stat.bin" {
			_ = json.NewEncoder(w).Encode(api.ListResponse{
				Objects: []api.ListObject{
					{ObjectID: "obj-stat-id", Name: "stat.bin"},
				},
			})
			return
		}
		if r.URL.Path == "/api/v1/objects/obj-stat-id/metadata" {
			_ = json.NewEncoder(w).Encode(api.ObjectDetail{
				Object: api.Object{
					ObjectID:       "obj-stat-id",
					Name:           "stat.bin",
					Size:           1048576,
					ContentType:    "application/octet-stream",
					ChecksumSHA256: "sha123456",
					ChunkCount:     1,
					CreatedAt:      "2026-10-21T00:00:00Z",
					ModifiedAt:     "2026-10-21T00:00:00Z",
					CustomMetadata: map[string]string{"env": "prod"},
				},
				Chunks: []api.ChunkInfo{
					{
						Index:   0,
						ChunkID: "obj-stat-id_0",
						Size:    1048576,
						SHA256:  "sha123456",
						Replicas: []api.ReplicaInfo{
							{WorkerID: "worker1", State: "OK"},
							{WorkerID: "worker2", State: "OK"},
						},
					},
				},
			})
			return
		}
		if r.URL.Path == "/api/v1/admin/status" {
			_ = json.NewEncoder(w).Encode(api.AdminStatus{
				Config: api.AdminConfig{ReplicationFactor: 2, WriteAcks: 2, ChunkSize: 1048576},
				Workers: []api.WorkerInfo{
					{WorkerID: "worker1", Status: "ALIVE", Address: "http://worker1:9000", UsedBytes: 1048576, ChunkCount: 1, LastSeenMSAgo: 500},
				},
				Objects: api.ObjectCounts{Committed: 1},
				Chunks:  api.ChunkCounts{Total: 1},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)

	// stat human
	code := app.Run([]string{"--endpoint", ts.URL, "stat", "obj:stat.bin"})
	if code != 0 {
		t.Fatalf("stat failed with %d: %s", code, stderr.String())
	}
	statOut := stdout.String()
	if !strings.Contains(statOut, "obj-stat-id") || !strings.Contains(statOut, "worker1 (OK)") {
		t.Errorf("unexpected stat output:\n%s", statOut)
	}

	// stat json
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "--json", "stat", "obj:stat.bin"})
	if code != 0 {
		t.Fatalf("stat --json failed with %d: %s", code, stderr.String())
	}
	var statJSON api.ObjectDetail
	if err := json.Unmarshal(stdout.Bytes(), &statJSON); err != nil {
		t.Fatalf("stat --json invalid JSON: %v", err)
	}

	// status human
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "status"})
	if code != 0 {
		t.Fatalf("status failed with %d: %s", code, stderr.String())
	}
	statusOut := stdout.String()
	if !strings.Contains(statusOut, "worker1") || !strings.Contains(statusOut, "Replication Factor: 2") {
		t.Errorf("unexpected status output:\n%s", statusOut)
	}

	// status json
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "status", "--json"})
	if code != 0 {
		t.Fatalf("status --json failed with %d: %s", code, stderr.String())
	}
	var adminStatus api.AdminStatus
	if err := json.Unmarshal(stdout.Bytes(), &adminStatus); err != nil {
		t.Fatalf("status --json invalid JSON: %v", err)
	}
}

func TestMetaCommands(t *testing.T) {
	currentMeta := map[string]string{"course": "cloud", "owner": "alice"}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/objects" && r.URL.Query().Get("name") == "doc.txt" {
			_ = json.NewEncoder(w).Encode(api.ListResponse{
				Objects: []api.ListObject{{ObjectID: "doc-id", Name: "doc.txt"}},
			})
			return
		}
		if r.URL.Path == "/api/v1/objects/doc-id/metadata" {
			if r.Method == http.MethodGet {
				_ = json.NewEncoder(w).Encode(api.ObjectDetail{
					Object: api.Object{ObjectID: "doc-id", Name: "doc.txt", CustomMetadata: currentMeta},
				})
				return
			}
			if r.Method == http.MethodPatch {
				var patch map[string]*string
				_ = json.NewDecoder(r.Body).Decode(&patch)
				for k, v := range patch {
					if v == nil {
						delete(currentMeta, k)
					} else {
						currentMeta[k] = *v
					}
				}
				_ = json.NewEncoder(w).Encode(api.ObjectDetail{
					Object: api.Object{ObjectID: "doc-id", Name: "doc.txt", CustomMetadata: currentMeta},
				})
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)

	// 1. meta get
	code := app.Run([]string{"--endpoint", ts.URL, "meta", "get", "obj:doc.txt"})
	if code != 0 {
		t.Fatalf("meta get failed: %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "course=cloud") {
		t.Errorf("meta get output mismatch: %s", stdout.String())
	}

	// 2. meta set
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "meta", "set", "obj:doc.txt", "course=infra"})
	if code != 0 {
		t.Fatalf("meta set failed: %d: %s", code, stderr.String())
	}
	if currentMeta["course"] != "infra" {
		t.Errorf("meta set failed to update: %+v", currentMeta)
	}

	// 3. meta rm
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "meta", "rm", "obj:doc.txt", "owner"})
	if code != 0 {
		t.Fatalf("meta rm failed: %d: %s", code, stderr.String())
	}
	if _, ok := currentMeta["owner"]; ok {
		t.Errorf("meta rm failed to delete owner: %+v", currentMeta)
	}
}

func TestVerifyCommand(t *testing.T) {
	data := []byte("verification matching data string")
	sum := sha256.Sum256(data)
	shaHex := hex.EncodeToString(sum[:])

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/objects" && r.URL.Query().Get("name") == "target.bin" {
			_ = json.NewEncoder(w).Encode(api.ListResponse{
				Objects: []api.ListObject{{ObjectID: "target-id", Name: "target.bin"}},
			})
			return
		}
		if r.URL.Path == "/api/v1/objects/target-id" {
			w.Header().Set("ETag", fmt.Sprintf("%q", shaHex))
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	matchingFile := filepath.Join(tmpDir, "match.bin")
	_ = os.WriteFile(matchingFile, data, 0644)

	mismatchFile := filepath.Join(tmpDir, "mismatch.bin")
	_ = os.WriteFile(mismatchFile, []byte("different content"), 0644)

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)

	// Test OK
	code := app.Run([]string{"--endpoint", ts.URL, "verify", "obj:target.bin", matchingFile})
	if code != 0 {
		t.Fatalf("expected verify OK (code 0), got %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected OK, got: %q", stdout.String())
	}

	// Test MISMATCH
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "verify", "obj:target.bin", mismatchFile})
	if code != 1 {
		t.Fatalf("expected verify MISMATCH (code 1), got %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "MISMATCH") {
		t.Errorf("expected MISMATCH, got: %q", stdout.String())
	}
}

func TestErrorHandlingAndExitCodes(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/objects" && r.URL.Query().Get("name") == "notfound.txt" {
			_ = json.NewEncoder(w).Encode(api.ListResponse{Objects: []api.ListObject{}})
			return
		}
		if r.URL.Path == "/api/v1/objects" && r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(api.ErrorBody{
				Error: api.ErrorDetail{Code: api.CodeNameConflict, Message: "object name already exists", RequestID: "r-test"},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)

	// 1. Not found (exit code 1)
	code := app.Run([]string{"--endpoint", ts.URL, "cat", "obj:notfound.txt"})
	if code != 1 {
		t.Fatalf("expected exit code 1 for not found, got %d", code)
	}
	if !strings.Contains(stderr.String(), "error: OBJECT_NOT_FOUND:") {
		t.Errorf("expected error: OBJECT_NOT_FOUND: ..., got: %q", stderr.String())
	}

	// 2. Conflict 409 (exit code 1)
	tmpFile := filepath.Join(t.TempDir(), "conflict.bin")
	_ = os.WriteFile(tmpFile, []byte("x"), 0644)
	stderr.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "cp", tmpFile, "obj:conflict.bin"})
	if code != 1 {
		t.Fatalf("expected exit code 1 for conflict, got %d", code)
	}
	if !strings.Contains(stderr.String(), "error: NAME_CONFLICT:") {
		t.Errorf("expected error: NAME_CONFLICT: ..., got: %q", stderr.String())
	}

	// 3. Usage error (exit code 2)
	stderr.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "cp"})
	if code != 2 {
		t.Fatalf("expected exit code 2 for usage error, got %d", code)
	}
	if !strings.Contains(stderr.String(), "error: INVALID_USAGE:") {
		t.Errorf("expected error: INVALID_USAGE: ..., got: %q", stderr.String())
	}
}

// TestEndToEndCLIWithRealController tests ocs cp up, cp down, ls, verify against real in-memory controller and datapath.
func TestEndToEndCLIWithRealController(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	cfg := controller.Config{
		ClusterToken:         "tok",
		ListenAddr:           ":8080",
		DBPath:               dbPath,
		ChunkSize:            1048576,
		ReplicationFactor:    2,
		WriteAcks:            2,
		UploadParallelism:    4,
		MaxConcurrentUploads: 16,
		MaxObjectBytes:       5368709120,
		DownloadPrefetch:     2,
		SuspectAfter:         6 * time.Second,
		DeadAfter:            15 * time.Second,
		RepairEnabled:        true,
		RepairConcurrency:    2,
	}

	reg := registry.New(store, cfg.SuspectAfter, cfg.DeadAfter, nil, nil)
	placer := placement.New(reg)
	fake := workerclient.NewFake()
	dp := datapath.New(cfg, store, reg, placer, fake, nil)
	ctrlServer := controller.NewServer(cfg, store, reg, placer, dp, nil)

	ctx := context.Background()
	for i := 1; i <= 4; i++ {
		wID := fmt.Sprintf("worker%d", i)
		_ = reg.Register(ctx, api.RegisterRequest{
			WorkerID:      wID,
			Address:       fmt.Sprintf("http://%s:9000", wID),
			CapacityBytes: 100 * 1024 * 1024,
		})
	}

	ts := httptest.NewServer(ctrlServer.Handler())
	defer ts.Close()

	tmpDir := t.TempDir()
	sampleFile := filepath.Join(tmpDir, "upload_sample.bin")
	sampleData := []byte("end-to-end full real pipeline test bytes 1234567890")
	_ = os.WriteFile(sampleFile, sampleData, 0644)

	var stdout, stderr bytes.Buffer
	app := cli.New(&stdout, &stderr, nil)

	// 1. cp upload
	code := app.Run([]string{"--endpoint", ts.URL, "cp", sampleFile, "obj:testdir/sample.bin", "--meta", "creator=test"})
	if code != 0 {
		t.Fatalf("cp upload failed with %d: %s", code, stderr.String())
	}

	// 2. ls hierarchy
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "ls"})
	if code != 0 {
		t.Fatalf("ls failed: %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "testdir/") {
		t.Errorf("expected directory prefix 'testdir/' in root ls, got:\n%s", stdout.String())
	}

	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "ls", "obj:testdir/"})
	if code != 0 {
		t.Fatalf("ls prefix failed: %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "testdir/sample.bin") {
		t.Errorf("expected 'testdir/sample.bin' in prefix ls, got:\n%s", stdout.String())
	}

	// 3. verify
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "verify", "obj:testdir/sample.bin", sampleFile})
	if code != 0 {
		t.Fatalf("verify failed with %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected verify OK, got: %s", stdout.String())
	}

	// 4. cp download
	downloadFile := filepath.Join(tmpDir, "downloaded_sample.bin")
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "cp", "obj:testdir/sample.bin", downloadFile})
	if code != 0 {
		t.Fatalf("cp download failed with %d: %s", code, stderr.String())
	}
	dlBytes, err := os.ReadFile(downloadFile)
	if err != nil || !bytes.Equal(dlBytes, sampleData) {
		t.Errorf("downloaded content mismatch, err: %v", err)
	}

	// 5. stat
	stdout.Reset()
	code = app.Run([]string{"--endpoint", ts.URL, "stat", "obj:testdir/sample.bin"})
	if code != 0 {
		t.Fatalf("stat failed with %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "creator=test") {
		t.Errorf("stat missing custom metadata: %s", stdout.String())
	}
}
