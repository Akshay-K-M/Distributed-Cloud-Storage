package workerclient

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/checksum"
)

// FakeChunk is one in-memory replica.
type FakeChunk struct {
	Data []byte
	SHA  string
	Size int64
}

// FakeClient is an in-memory Client with failure-injection hooks.
type FakeClient struct {
	mu     sync.Mutex
	chunks map[string]map[string]FakeChunk // workerID -> chunkID -> chunk
	q      map[string]map[string]struct{}  // workerID -> quarantined ids

	BeforePut func(w WorkerRef, chunkID string) error
	BeforeGet func(w WorkerRef, chunkID string) error
	BeforeDel func(w WorkerRef, chunkID string) error
	CorruptGet func(w WorkerRef, chunkID string, data []byte) []byte
	PutDelay   time.Duration
	GetDelay   time.Duration
}

// NewFake returns an empty FakeClient.
func NewFake() *FakeClient {
	return &FakeClient{
		chunks: make(map[string]map[string]FakeChunk),
		q:      make(map[string]map[string]struct{}),
	}
}

func (f *FakeClient) PutChunk(ctx context.Context, w WorkerRef, chunkID, sha256hex string, size int64, body io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.PutDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.PutDelay):
		}
	}
	if f.BeforePut != nil {
		if err := f.BeforePut(w, chunkID); err != nil {
			return err
		}
	}
	b, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return err
	}
	if int64(len(b)) != size || !checksum.EqualHex(checksum.SHA256Hex(b), sha256hex) {
		return ErrChecksumMismatch
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.chunks[w.ID] == nil {
		f.chunks[w.ID] = make(map[string]FakeChunk)
	}
	f.chunks[w.ID][chunkID] = FakeChunk{Data: append([]byte(nil), b...), SHA: checksum.SHA256Hex(b), Size: size}
	if f.q[w.ID] != nil {
		delete(f.q[w.ID], chunkID)
	}
	return nil
}

func (f *FakeClient) GetChunk(ctx context.Context, w WorkerRef, chunkID string) (io.ReadCloser, int64, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, "", err
	}
	if f.GetDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, 0, "", ctx.Err()
		case <-time.After(f.GetDelay):
		}
	}
	if f.BeforeGet != nil {
		if err := f.BeforeGet(w, chunkID); err != nil {
			return nil, 0, "", err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, q := f.q[w.ID][chunkID]; q {
		return nil, 0, "", ErrChunkNotFound
	}
	ch, ok := f.chunks[w.ID][chunkID]
	if !ok {
		return nil, 0, "", ErrChunkNotFound
	}
	data := append([]byte(nil), ch.Data...)
	if f.CorruptGet != nil {
		data = f.CorruptGet(w, chunkID, data)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), ch.SHA, nil
}

func (f *FakeClient) DeleteChunk(ctx context.Context, w WorkerRef, chunkID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.BeforeDel != nil {
		if err := f.BeforeDel(w, chunkID); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.chunks[w.ID] != nil {
		delete(f.chunks[w.ID], chunkID)
	}
	if f.q[w.ID] != nil {
		delete(f.q[w.ID], chunkID)
	}
	return nil
}

func (f *FakeClient) ListChunks(ctx context.Context, w WorkerRef) (*api.Inventory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	inv := &api.Inventory{WorkerID: w.ID, Chunks: []api.InventoryChunk{}, Quarantined: []string{}}
	for id, ch := range f.chunks[w.ID] {
		inv.Chunks = append(inv.Chunks, api.InventoryChunk{ChunkID: id, Size: ch.Size, SHA256: ch.SHA})
	}
	for id := range f.q[w.ID] {
		inv.Quarantined = append(inv.Quarantined, id)
	}
	return inv, nil
}

func (f *FakeClient) Scrub(ctx context.Context, w WorkerRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Quarantine marks a chunk as quarantined (404 on get).
func (f *FakeClient) Quarantine(workerID, chunkID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.q[workerID] == nil {
		f.q[workerID] = make(map[string]struct{})
	}
	f.q[workerID][chunkID] = struct{}{}
	if f.chunks[workerID] != nil {
		delete(f.chunks[workerID], chunkID)
	}
}
