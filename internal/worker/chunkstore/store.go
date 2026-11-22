// Package chunkstore persists chunks by ID and verifies their SHA-256 digests.
package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const DefaultMaxChunk int64 = 1048576

var (
	ErrNotFound         = errors.New("chunk not found")
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrInvalidID        = errors.New("invalid chunk id")
	ErrTooLarge         = errors.New("chunk too large")
	idPattern           = regexp.MustCompile(`^[A-Za-z0-9_-]{1,200}$`)
)

// Meta is the on-disk sidecar and in-memory index record.
type Meta struct {
	ChunkID   string `json:"-"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	CreatedAt int64  `json:"created_at"`
}

// Store is the worker on-disk chunk store.
type Store struct {
	root          string
	chunksDir     string
	tmpDir        string
	quarantineDir string
	maxChunk      int64

	mu          sync.RWMutex
	index       map[string]Meta
	quarantined map[string]struct{}
	locks       [64]sync.Mutex // Fixed stripes bound memory as chunk IDs come and go.
}

// Open mkdirs, cleans tmp/, scans chunks/, builds the index without rehashing.
func Open(dir string) (*Store, error) {
	return OpenWithMaxChunk(dir, DefaultMaxChunk)
}

// OpenWithMaxChunk applies the same limit used by the worker HTTP handler.
func OpenWithMaxChunk(dir string, maxChunk int64) (*Store, error) {
	s := &Store{
		root:          dir,
		chunksDir:     filepath.Join(dir, "chunks"),
		tmpDir:        filepath.Join(dir, "tmp"),
		quarantineDir: filepath.Join(dir, "quarantine"),
		maxChunk:      maxChunk,
		index:         make(map[string]Meta),
		quarantined:   make(map[string]struct{}),
	}
	for _, d := range []string{s.chunksDir, s.tmpDir, s.quarantineDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := cleanDir(s.tmpDir); err != nil {
		return nil, err
	}
	if err := s.scan(); err != nil {
		return nil, err
	}
	return s, nil
}

// ValidID is the path-traversal guard.
func ValidID(id string) bool {
	return idPattern.MatchString(id)
}

// DirShards returns the first 4 hex chars of sha256(chunk_id) as two dirs.
func DirShards(id string) (string, string) {
	sum := sha256.Sum256([]byte(id))
	h := hex.EncodeToString(sum[:])
	return h[:2], h[2:4]
}

func (s *Store) dataPath(id string) string {
	a, b := DirShards(id)
	return filepath.Join(s.chunksDir, a, b, id)
}

func (s *Store) metaPath(id string) string {
	return s.dataPath(id) + ".meta"
}

func (s *Store) lockID(id string) func() {
	sum := sha256.Sum256([]byte(id))
	m := &s.locks[int(sum[0])%len(s.locks)]
	m.Lock()
	return m.Unlock
}

// Put streams r to disk, verifies length and SHA-256, then atomically installs it.
func (s *Store) Put(ctx context.Context, id, wantSHA string, wantSize int64, r io.Reader) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !ValidID(id) {
		return false, ErrInvalidID
	}
	if wantSize < 0 || wantSize > s.maxChunk {
		return false, ErrTooLarge
	}

	unlock := s.lockID(id)
	defer unlock()

	tmp, err := os.CreateTemp(s.tmpDir, "put-*.dat")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}

	hasher := sha256.New()
	n, err := io.Copy(tmp, io.TeeReader(io.LimitReader(&ctxReader{ctx: ctx, r: r}, wantSize+1), hasher))
	if err != nil {
		cleanup()
		return false, err
	}
	gotSHA := hex.EncodeToString(hasher.Sum(nil))
	if n != wantSize || !strings.EqualFold(gotSHA, wantSHA) {
		cleanup()
		return false, ErrChecksumMismatch
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return false, err
	}

	meta := Meta{
		ChunkID:   id,
		Size:      wantSize,
		SHA256:    gotSHA,
		CreatedAt: time.Now().UnixMilli(),
	}
	metaTmp, err := os.CreateTemp(s.tmpDir, "meta-*.json")
	if err != nil {
		_ = os.Remove(tmpName)
		return false, err
	}
	metaTmpName := metaTmp.Name()
	if err := json.NewEncoder(metaTmp).Encode(meta); err != nil {
		_ = metaTmp.Close()
		_ = os.Remove(metaTmpName)
		_ = os.Remove(tmpName)
		return false, err
	}
	if err := metaTmp.Sync(); err != nil {
		_ = metaTmp.Close()
		_ = os.Remove(metaTmpName)
		_ = os.Remove(tmpName)
		return false, err
	}
	if err := metaTmp.Close(); err != nil {
		_ = os.Remove(metaTmpName)
		_ = os.Remove(tmpName)
		return false, err
	}

	finalData := s.dataPath(id)
	finalMeta := s.metaPath(id)
	if err := os.MkdirAll(filepath.Dir(finalData), 0o755); err != nil {
		_ = os.Remove(tmpName)
		_ = os.Remove(metaTmpName)
		return false, err
	}

	s.mu.RLock()
	_, existed := s.index[id]
	s.mu.RUnlock()
	if _, err := os.Stat(finalData); err == nil {
		existed = true
	}

	if err := os.Rename(tmpName, finalData); err != nil {
		_ = os.Remove(tmpName)
		_ = os.Remove(metaTmpName)
		return false, err
	}
	if err := os.Rename(metaTmpName, finalMeta); err != nil {
		_ = os.Remove(metaTmpName)
		_ = os.Remove(finalData)
		return false, err
	}
	if err := fsyncDir(filepath.Dir(finalData)); err != nil {
		return false, err
	}

	s.mu.Lock()
	s.index[id] = meta
	delete(s.quarantined, id)
	s.mu.Unlock()
	_ = os.Remove(filepath.Join(s.quarantineDir, id))
	return !existed, nil
}

// Open returns a seekable reader for a live (non-quarantined) chunk.
func (s *Store) Open(id string) (io.ReadSeekCloser, Meta, error) {
	if !ValidID(id) {
		return nil, Meta{}, ErrInvalidID
	}
	s.mu.RLock()
	meta, ok := s.index[id]
	_, q := s.quarantined[id]
	s.mu.RUnlock()
	if !ok || q {
		return nil, Meta{}, ErrNotFound
	}
	f, err := os.Open(s.dataPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, Meta{}, ErrNotFound
		}
		return nil, Meta{}, err
	}
	return f, meta, nil
}

// Delete is idempotent and removes data, sidecar, and any quarantined copy.
func (s *Store) Delete(id string) error {
	if id == "" || !ValidID(id) {
		return nil
	}
	unlock := s.lockID(id)
	defer unlock()
	for _, path := range []string{s.dataPath(id), s.metaPath(id), filepath.Join(s.quarantineDir, id)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err // Keep the index entry so a later delete can retry.
		}
	}
	s.mu.Lock()
	delete(s.index, id)
	delete(s.quarantined, id)
	s.mu.Unlock()
	return nil
}

// Inventory returns live chunk metadata and quarantined ids.
func (s *Store) Inventory() (chunks []Meta, quarantined []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chunks = make([]Meta, 0, len(s.index))
	for _, m := range s.index {
		chunks = append(chunks, m)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkID < chunks[j].ChunkID })
	quarantined = make([]string, 0, len(s.quarantined))
	for id := range s.quarantined {
		quarantined = append(quarantined, id)
	}
	sort.Strings(quarantined)
	return chunks, quarantined
}

// Stats returns used bytes, live chunk count, and filesystem capacity.
func (s *Store) Stats() (usedBytes int64, chunkCount int, capacityBytes int64) {
	s.mu.RLock()
	for _, m := range s.index {
		usedBytes += m.Size
	}
	chunkCount = len(s.index)
	s.mu.RUnlock()
	return usedBytes, chunkCount, diskCapacity(s.root)
}

// Scrub rehashes every live chunk, throttled to rate bytes/sec (0 = unlimited).
func (s *Store) Scrub(ctx context.Context, rate int64) []string {
	s.mu.RLock()
	ids := make([]string, 0, len(s.index))
	for id := range s.index {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Strings(ids)

	tb := newBucket(rate)
	var bad []string
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return bad
		}
		// Verify and quarantine under the same lock as PUT. A repair that
		// arrives during the scan must never be moved aside as the old bad copy.
		unlock := s.lockID(id)
		if !s.verifyOne(ctx, id, tb) {
			s.quarantine(id)
			bad = append(bad, id)
		}
		unlock()
	}
	return bad
}

func (s *Store) verifyOne(ctx context.Context, id string, tb *tokenBucket) bool {
	s.mu.RLock()
	meta, ok := s.index[id]
	s.mu.RUnlock()
	if !ok {
		return true // chunk was removed or quarantined concurrently
	}
	f, err := os.Open(s.dataPath(id))
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() != meta.Size {
		return false
	}
	h := sha256.New()
	buf := make([]byte, 32*1024)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return true // abort without quarantine
		}
		nr, er := f.Read(buf)
		if nr > 0 {
			_, _ = h.Write(buf[:nr])
			n += int64(nr)
			tb.take(ctx, int64(nr))
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			return false
		}
	}
	got := hex.EncodeToString(h.Sum(nil))
	return n == meta.Size && strings.EqualFold(got, meta.SHA256)
}

// Caller holds the chunk lock.
func (s *Store) quarantine(id string) {
	s.mu.RLock()
	_, ok := s.index[id]
	s.mu.RUnlock()
	if !ok {
		return
	}
	src := s.dataPath(id)
	dst := filepath.Join(s.quarantineDir, id)
	_ = os.MkdirAll(s.quarantineDir, 0o755)
	if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
		_ = copyFile(src, dst)
		_ = os.Remove(src)
	}
	_ = os.Remove(s.metaPath(id))
	s.mu.Lock()
	delete(s.index, id)
	s.quarantined[id] = struct{}{}
	s.mu.Unlock()
}

func (s *Store) scan() error {
	entries, err := os.ReadDir(s.quarantineDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				s.quarantined[e.Name()] = struct{}{}
			}
		}
	}

	return filepath.WalkDir(s.chunksDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if filepath.Ext(name) == ".meta" {
			id := name[:len(name)-len(".meta")]
			data := filepath.Join(filepath.Dir(path), id)
			if _, err := os.Stat(data); err != nil {
				_ = os.Remove(path)
			}
			return nil
		}
		metaFile := path + ".meta"
		if _, err := os.Stat(metaFile); err != nil {
			_ = os.Remove(path)
			return nil
		}
		if !ValidID(name) {
			_ = os.Remove(path)
			_ = os.Remove(metaFile)
			return nil
		}
		if _, isQ := s.quarantined[name]; isQ {
			_ = os.Remove(path)
			_ = os.Remove(metaFile)
			return nil
		}
		b, err := os.ReadFile(metaFile)
		if err != nil {
			return err
		}
		var m Meta
		if err := json.Unmarshal(b, &m); err != nil {
			_ = os.Remove(path)
			_ = os.Remove(metaFile)
			return nil
		}
		m.ChunkID = name
		s.index[name] = m
		return nil
	})
}

func cleanDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(dir, e.Name()))
	}
	return nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func diskCapacity(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	if st.Blocks > 0 {
		return int64(st.Blocks) * int64(st.Bsize)
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

type tokenBucket struct {
	rate   float64
	tokens float64
	last   time.Time
}

func newBucket(rate int64) *tokenBucket {
	if rate <= 0 {
		return &tokenBucket{}
	}
	return &tokenBucket{rate: float64(rate), tokens: float64(rate), last: time.Now()}
}

func (b *tokenBucket) take(ctx context.Context, n int64) {
	if b == nil || b.rate <= 0 {
		return
	}
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.rate {
		b.tokens = b.rate
	}
	b.last = now
	need := float64(n)
	if b.tokens >= need {
		b.tokens -= need
		return
	}
	deficit := need - b.tokens
	sleep := time.Duration(deficit / b.rate * float64(time.Second))
	t := time.NewTimer(sleep)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
	}
	b.tokens = 0
	b.last = time.Now()
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
