// This file chunks uploads, writes replicas, and commits object metadata.
package datapath

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
	"objstore/internal/common/ids"
	"objstore/internal/controller/metadata"
	"objstore/internal/controller/placement"
	"objstore/internal/workerclient"
)

var validMetaKey = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func validateObjectName(name string) error {
	if len(name) < 1 {
		return errors.New("object name is required")
	}
	if len(name) > 1024 {
		return errors.New("object name must not exceed 1024 bytes")
	}
	if !utf8.ValidString(name) {
		return errors.New("object name must be valid UTF-8")
	}
	if strings.HasPrefix(name, "/") {
		return errors.New("object name must not start with /")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return errors.New("object name must not contain control characters")
		}
	}
	return nil
}

func validateCustomMetadata(meta map[string]string) error {
	if len(meta) > 64 {
		return errors.New("custom metadata cannot exceed 64 keys")
	}
	for k, v := range meta {
		if !validMetaKey.MatchString(k) {
			return errors.New("invalid custom metadata key format")
		}
		if len(v) > 1024 {
			return errors.New("custom metadata value must not exceed 1024 bytes")
		}
	}
	return nil
}

func detectContentType(filePartContentType, name string, firstChunk []byte) string {
	ct := strings.TrimSpace(filePartContentType)
	if ct != "" && ct != "application/octet-stream" {
		return ct
	}
	if ext := filepath.Ext(name); ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			return mimeType
		}
	}
	if len(firstChunk) > 0 {
		return http.DetectContentType(firstChunk)
	}
	return "application/octet-stream"
}

func newTempID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type writtenItem struct {
	workerID string
	chunkID  string
	size     int64
}

type writtenTracker struct {
	mu    sync.Mutex
	items []writtenItem
}

func (t *writtenTracker) add(workerID, chunkID string, size int64) {
	t.mu.Lock()
	t.items = append(t.items, writtenItem{workerID: workerID, chunkID: chunkID, size: size})
	t.mu.Unlock()
}

func (t *writtenTracker) list() []writtenItem {
	t.mu.Lock()
	defer t.mu.Unlock()
	res := make([]writtenItem, len(t.items))
	copy(res, t.items)
	return res
}

type resultMap struct {
	mu      sync.Mutex
	commits map[int]metadata.ChunkCommit
}

func newResultMap() *resultMap {
	return &resultMap{commits: make(map[int]metadata.ChunkCommit)}
}

func (m *resultMap) set(idx int, c metadata.ChunkCommit) {
	m.mu.Lock()
	m.commits[idx] = c
	m.mu.Unlock()
}

func (m *resultMap) sorted() []metadata.ChunkCommit {
	m.mu.Lock()
	defer m.mu.Unlock()
	sorted := make([]metadata.ChunkCommit, len(m.commits))
	for idx, c := range m.commits {
		sorted[idx] = c
	}
	return sorted
}

// Upload handles POST /api/v1/objects.
func (d *DataPath) Upload(w http.ResponseWriter, r *http.Request) {
	// 1. Admission control via semaphore
	select {
	case d.uploadSem <- struct{}{}:
		defer func() { <-d.uploadSem }()
	default:
		w.Header().Set("Retry-After", "2")
		httpx.WriteError(w, r, api.CodeOverloaded, "server overloaded")
		return
	}

	// Wrap body with MaxBytesReader (allow 10 MiB overhead for multipart boundaries/headers)
	r.Body = http.MaxBytesReader(w, r.Body, d.cfg.MaxObjectBytes+10*1024*1024)

	mr, err := r.MultipartReader()
	if err != nil {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid multipart request")
		return
	}

	overwrite := r.URL.Query().Get("overwrite") == "true"

	var (
		name                string
		customMeta          map[string]string
		fileSeen            bool
		objectID            string
		chunkCommits        []metadata.ChunkCommit
		totalBytes          int64
		objChecksum         string
		detectedContentType string
		uploadErr           error
		fileContentType     string
	)

	tracker := &writtenTracker{}

	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				httpx.WriteError(w, r, api.CodeObjectTooLarge, "object exceeds maximum allowed size")
				return
			}
			httpx.WriteError(w, r, api.CodeInvalidRequest, "error reading multipart form")
			return
		}

		formName := part.FormName()
		switch formName {
		case "name":
			b, readErr := io.ReadAll(io.LimitReader(part, 64*1024+1))
			if readErr != nil || len(b) > 64*1024 {
				httpx.WriteError(w, r, api.CodeInvalidRequest, "name part exceeds 64 KiB")
				return
			}
			name = string(b)
		case "metadata":
			b, readErr := io.ReadAll(io.LimitReader(part, 64*1024+1))
			if readErr != nil || len(b) > 64*1024 {
				httpx.WriteError(w, r, api.CodeInvalidRequest, "metadata part exceeds 64 KiB")
				return
			}
			var meta map[string]string
			if err := json.Unmarshal(b, &meta); err != nil {
				httpx.WriteError(w, r, api.CodeInvalidRequest, "invalid custom metadata JSON")
				return
			}
			if err := validateCustomMetadata(meta); err != nil {
				httpx.WriteError(w, r, api.CodeInvalidRequest, err.Error())
				return
			}
			customMeta = meta
		case "file":
			fileSeen = true
			if name == "" {
				name = part.FileName()
			}

			// Validate name early if known
			provisionalName := name
			if provisionalName != "" {
				if err := validateObjectName(provisionalName); err != nil {
					httpx.WriteError(w, r, api.CodeInvalidRequest, err.Error())
					return
				}
			} else {
				provisionalName = "upload-" + newTempID()
			}

			fileContentType = part.Header.Get("Content-Type")

			objID, err := d.store.BeginUpload(r.Context(), provisionalName, fileContentType, overwrite)
			if errors.Is(err, metadata.ErrNameConflict) {
				httpx.WriteError(w, r, api.CodeNameConflict, "object name already exists")
				return
			}
			if err != nil {
				d.log.Error("begin upload failed", "err", err)
				httpx.WriteError(w, r, api.CodeInternal, "failed to begin upload")
				return
			}
			objectID = objID
			d.inFlight.Store(objectID, struct{}{})
			defer d.inFlight.Delete(objectID)

			var firstChunk []byte
			chunkCommits, totalBytes, objChecksum, firstChunk, uploadErr = d.streamFileChunks(r.Context(), objectID, part, tracker)
			if uploadErr != nil {
				break
			}

			detectedContentType = detectContentType(fileContentType, name, firstChunk)
		default:
			_, _ = io.Copy(io.Discard, io.LimitReader(part, 1<<20))
		}

		if uploadErr != nil {
			break
		}
	}

	if uploadErr != nil {
		d.abortAndCleanup(objectID, tracker)
		if r.Context().Err() != nil {
			d.log.Info("client disconnected during upload", "object_id", objectID, "err", uploadErr)
			return
		}
		if errors.Is(uploadErr, errObjectTooLarge) {
			httpx.WriteError(w, r, api.CodeObjectTooLarge, "object exceeds maximum allowed size")
			return
		}
		if errors.Is(uploadErr, placement.ErrInsufficientWorkers) || errors.Is(uploadErr, errInsufficientReplicas) {
			httpx.WriteError(w, r, api.CodeStorageUnavailable, "insufficient storage workers")
			return
		}
		httpx.WriteError(w, r, api.CodeStorageUnavailable, "failed to store chunk")
		return
	}

	if !fileSeen {
		httpx.WriteError(w, r, api.CodeInvalidRequest, "file part is required")
		return
	}

	if name == "" {
		d.abortAndCleanup(objectID, tracker)
		httpx.WriteError(w, r, api.CodeInvalidRequest, "object name is required")
		return
	}

	if err := validateObjectName(name); err != nil {
		d.abortAndCleanup(objectID, tracker)
		httpx.WriteError(w, r, api.CodeInvalidRequest, err.Error())
		return
	}

	if err := validateCustomMetadata(customMeta); err != nil {
		d.abortAndCleanup(objectID, tracker)
		httpx.WriteError(w, r, api.CodeInvalidRequest, err.Error())
		return
	}

	// Commit object in SQLite
	commit := metadata.CommitInput{
		ObjectID:       objectID,
		Name:           name,
		Size:           totalBytes,
		ChecksumSHA256: objChecksum,
		ChunkCount:     len(chunkCommits),
		ContentType:    detectedContentType,
		CustomMetadata: customMeta,
		Overwrite:      overwrite,
		Chunks:         chunkCommits,
	}

	committedObj, err := d.store.CommitObject(r.Context(), commit)
	if errors.Is(err, metadata.ErrNameConflict) {
		d.abortAndCleanup(objectID, tracker)
		httpx.WriteError(w, r, api.CodeNameConflict, "object name already exists")
		return
	}
	if err != nil {
		d.abortAndCleanup(objectID, tracker)
		d.log.Error("commit object failed", "err", err, "object_id", objectID)
		httpx.WriteError(w, r, api.CodeInternal, "failed to commit object")
		return
	}

	w.Header().Set("Location", "/api/v1/objects/"+committedObj.ObjectID)
	httpx.WriteJSON(w, http.StatusCreated, committedObj)
}

func (d *DataPath) streamFileChunks(
	ctx context.Context,
	objectID string,
	filePart io.Reader,
	tracker *writtenTracker,
) ([]metadata.ChunkCommit, int64, string, []byte, error) {
	g, gctx := errgroup.WithContext(ctx)
	window := make(chan struct{}, d.cfg.UploadParallelism)
	objHash := sha256.New()

	var total int64
	var firstChunk []byte
	results := newResultMap()
	var readErr error

	for idx := 0; ; idx++ {
		select {
		case window <- struct{}{}:
		case <-gctx.Done():
			readErr = gctx.Err()
			goto wait
		}

		bPtr := d.bufPool.Get().(*[]byte)
		if int64(len(*bPtr)) != d.cfg.ChunkSize {
			*bPtr = (*bPtr)[:d.cfg.ChunkSize]
		}

		n, err := io.ReadFull(filePart, *bPtr)
		var lastChunk bool

		if errors.Is(err, io.EOF) {
			if idx > 0 && n == 0 {
				<-window
				d.bufPool.Put(bPtr)
				break
			}
			if idx == 0 && n == 0 {
				// Zero-byte object: ONE empty chunk
				lastChunk = true
			}
		} else if errors.Is(err, io.ErrUnexpectedEOF) {
			lastChunk = true
		} else if err != nil {
			<-window
			d.bufPool.Put(bPtr)
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				readErr = errObjectTooLarge
			} else {
				readErr = err
			}
			break
		}

		if idx == 0 {
			snipLen := min(n, 512)
			firstChunk = append([]byte(nil), (*bPtr)[:snipLen]...)
		}

		objHash.Write((*bPtr)[:n])
		total += int64(n)
		if total > d.cfg.MaxObjectBytes {
			<-window
			d.bufPool.Put(bPtr)
			readErr = errObjectTooLarge
			break
		}

		sum := sha256.Sum256((*bPtr)[:n])
		i := idx
		data := (*bPtr)[:n]

		g.Go(func() error {
			defer func() {
				<-window
				d.bufPool.Put(bPtr)
			}()
			rec, sErr := d.storeChunk(gctx, objectID, i, data, sum, tracker)
			if sErr != nil {
				return sErr
			}
			results.set(i, rec)
			return nil
		})

		if lastChunk {
			break
		}
	}

wait:
	if waitErr := g.Wait(); waitErr != nil {
		return nil, 0, "", nil, waitErr
	}
	if readErr != nil {
		return nil, 0, "", nil, readErr
	}

	ordered := results.sorted()
	return ordered, total, hex.EncodeToString(objHash.Sum(nil)), firstChunk, nil
}

func (d *DataPath) storeChunk(
	ctx context.Context,
	objectID string,
	idx int,
	data []byte,
	sum [32]byte,
	tracker *writtenTracker,
) (metadata.ChunkCommit, error) {
	chunkID := ids.ChunkID(objectID, idx)
	shaHex := hex.EncodeToString(sum[:])
	size := int64(len(data))

	targets, err := d.placer.Pick(d.cfg.WriteAcks, nil, size)
	if err != nil {
		return metadata.ChunkCommit{}, err
	}

	tried := make(map[string]bool)
	for _, t := range targets {
		tried[t.ID] = true
	}

	type putResult struct {
		workerID string
		err      error
	}

	resultsCh := make(chan putResult, len(targets))
	for _, target := range targets {
		t := target
		go func() {
			pErr := d.client.PutChunk(ctx, workerclient.WorkerRef{ID: t.ID, Addr: t.Addr}, chunkID, shaHex, size, bytes.NewReader(data))
			resultsCh <- putResult{workerID: t.ID, err: pErr}
		}()
	}

	var successful []string
	var failedCount int

	for i := 0; i < len(targets); i++ {
		res := <-resultsCh
		if res.err == nil {
			successful = append(successful, res.workerID)
			tracker.add(res.workerID, chunkID, size)
		} else {
			d.log.Warn("put chunk to worker failed", "worker_id", res.workerID, "chunk_id", chunkID, "err", res.err)
			d.placer.Release(res.workerID, size)
			failedCount++
		}
	}

	// Retry once on different workers for failed targets
	for i := 0; i < failedCount; i++ {
		replacement, pickErr := d.placer.Pick(1, tried, size)
		if pickErr != nil || len(replacement) == 0 {
			break
		}
		rep := replacement[0]
		tried[rep.ID] = true

		pErr := d.client.PutChunk(ctx, workerclient.WorkerRef{ID: rep.ID, Addr: rep.Addr}, chunkID, shaHex, size, bytes.NewReader(data))
		if pErr == nil {
			successful = append(successful, rep.ID)
			tracker.add(rep.ID, chunkID, size)
		} else {
			d.log.Warn("retry put chunk failed", "worker_id", rep.ID, "chunk_id", chunkID, "err", pErr)
			d.placer.Release(rep.ID, size)
		}
	}

	if len(successful) < d.cfg.WriteAcks {
		return metadata.ChunkCommit{}, errInsufficientReplicas
	}

	return metadata.ChunkCommit{
		ChunkID:  chunkID,
		Index:    idx,
		Size:     size,
		SHA256:   shaHex,
		Replicas: successful,
	}, nil
}

func (d *DataPath) abortAndCleanup(objectID string, tracker *writtenTracker) {
	if objectID != "" {
		_ = d.store.AbortUpload(context.Background(), objectID)
	}
	items := tracker.list()
	if len(items) == 0 {
		return
	}
	go func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, it := range items {
			wInfo, ok := d.registry.Get(it.workerID)
			if ok {
				_ = d.client.DeleteChunk(delCtx, workerclient.WorkerRef{ID: wInfo.ID, Addr: wInfo.Addr}, it.chunkID)
			}
			d.placer.Release(it.workerID, it.size)
		}
	}()
}
