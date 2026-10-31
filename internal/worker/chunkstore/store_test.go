// These tests verify chunk persistence, checksums, quarantine, and idempotence.
package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func openT(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func putOK(t *testing.T, s *Store, id string, data []byte) {
	t.Helper()
	created, err := s.Put(context.Background(), id, sha(data), int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected created")
	}
}

func readAll(t *testing.T, s *Store, id string) []byte {
	t.Helper()
	rc, _, err := s.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty %s, got %v", dir, entries)
	}
}

func TestPutOpenDeleteRoundTrip(t *testing.T) {
	s := openT(t)
	ctx := context.Background()

	cases := [][]byte{
		{},
		[]byte("hello"),
		bytes.Repeat([]byte{0xab}, int(DefaultMaxChunk)),
	}
	ids := []string{"empty_0", "small_0", "full_0"}
	for i, data := range cases {
		id := ids[i]
		putOK(t, s, id, data)
		got := readAll(t, s, id)
		if !bytes.Equal(got, data) {
			t.Fatalf("%s: mismatch", id)
		}
		if err := s.Delete(id); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Open(id); err != ErrNotFound {
			t.Fatalf("deleted still openable: %v", err)
		}
		if err := s.Delete(id); err != nil {
			t.Fatal(err)
		}
	}

	tooBig := bytes.Repeat([]byte{1}, int(DefaultMaxChunk)+1)
	_, err := s.Put(ctx, "big_0", sha(tooBig), int64(len(tooBig)), bytes.NewReader(tooBig))
	if err != ErrTooLarge {
		t.Fatalf("1MiB+1: want ErrTooLarge, got %v", err)
	}
}

func TestChecksumAndLengthRejected(t *testing.T) {
	s := openT(t)
	ctx := context.Background()
	data := bytes.Repeat([]byte("x"), 100)

	_, err := s.Put(ctx, "c_0", "deadbeef", int64(len(data)), bytes.NewReader(data))
	if err != ErrChecksumMismatch {
		t.Fatalf("wrong checksum: %v", err)
	}

	_, err = s.Put(ctx, "c_0", sha(data), int64(len(data)+5), bytes.NewReader(data))
	if err != ErrChecksumMismatch {
		t.Fatalf("wrong length: %v", err)
	}

	_, err = s.Put(ctx, "c_0", sha(data[:50]), 50, bytes.NewReader(data[:40]))
	if err != ErrChecksumMismatch {
		t.Fatalf("truncated: %v", err)
	}

	assertEmpty(t, s.tmpDir)
	_ = filepath.WalkDir(s.chunksDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			t.Fatalf("leftover file %s", path)
		}
		return nil
	})
}

func TestInvalidIDs(t *testing.T) {
	s := openT(t)
	ctx := context.Background()
	data := []byte("x")
	for _, id := range []string{"../x", "a/b", "", "foo/bar", "id with space", "a.b"} {
		_, err := s.Put(ctx, id, sha(data), 1, bytes.NewReader(data))
		if err != ErrInvalidID {
			t.Fatalf("id %q: want ErrInvalidID, got %v", id, err)
		}
	}
}

func TestCrashCleanupOnReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("keep-me")
	putOK(t, s, "keep_0", data)

	orphan := "orphan_0"
	a, b := DirShards(orphan)
	orphanDir := filepath.Join(dir, "chunks", a, b)
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanDir, orphan), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tmp", "stray.dat"), []byte("tmp"), 0o644); err != nil {
		t.Fatal(err)
	}
	lonelyMeta := "lonely_0"
	la, lb := DirShards(lonelyMeta)
	ld := filepath.Join(dir, "chunks", la, lb)
	if err := os.MkdirAll(ld, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ld, lonelyMeta+".meta"), []byte(`{"size":1,"sha256":"aa","created_at":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertEmpty(t, s2.tmpDir)
	if _, err := os.Stat(filepath.Join(orphanDir, orphan)); !os.IsNotExist(err) {
		t.Fatal("orphan data should be gone")
	}
	if _, err := os.Stat(filepath.Join(ld, lonelyMeta+".meta")); !os.IsNotExist(err) {
		t.Fatal("lonely meta should be gone")
	}
	got := readAll(t, s2, "keep_0")
	if !bytes.Equal(got, data) {
		t.Fatal("good chunk lost")
	}
}

func TestConcurrentPutSameID(t *testing.T) {
	s := openT(t)
	data := bytes.Repeat([]byte("z"), 4096)
	sum := sha(data)
	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Put(context.Background(), "same_0", sum, int64(len(data)), bytes.NewReader(data))
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	got := readAll(t, s, "same_0")
	if !bytes.Equal(got, data) {
		t.Fatal("final content invalid")
	}
}

func TestScrubQuarantine(t *testing.T) {
	s := openT(t)
	data := bytes.Repeat([]byte("k"), 1024)
	putOK(t, s, "good_0", data)
	putOK(t, s, "flip_0", data)
	putOK(t, s, "trunc_0", data)

	flipPath := s.dataPath("flip_0")
	b, err := os.ReadFile(flipPath)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(flipPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(s.dataPath("trunc_0"), 10); err != nil {
		t.Fatal(err)
	}

	bad := s.Scrub(context.Background(), 0)
	if len(bad) != 2 {
		t.Fatalf("bad=%v", bad)
	}
	if _, _, err := s.Open("flip_0"); err != ErrNotFound {
		t.Fatal("flipped still served")
	}
	if _, _, err := s.Open("trunc_0"); err != ErrNotFound {
		t.Fatal("truncated still served")
	}
	if _, _, err := s.Open("good_0"); err != nil {
		t.Fatal(err)
	}
	chunks, q := s.Inventory()
	if len(chunks) != 1 || chunks[0].ChunkID != "good_0" {
		t.Fatalf("inventory chunks=%v", chunks)
	}
	if len(q) != 2 {
		t.Fatalf("quarantined=%v", q)
	}
}
