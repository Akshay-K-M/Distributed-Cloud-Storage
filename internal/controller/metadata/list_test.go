// These tests verify listing filters, delimiters, and cursor pagination.
package metadata_test

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"objstore/internal/controller/metadata"
)

func TestStore_ListFixture(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "list_test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open failed: %v", err)
	}
	defer store.Close()

	names := []string{
		"a.txt",
		"b.txt",
		"docs/2025/report.pdf",
		"docs/2026/plan.pdf",
		"docs/guide.md",
		"docs/notes.txt",
		"photos/2025/vacation.jpg",
		"photos/2026/a.jpg",
		"photos/2026/b.jpg",
		"photos/cat.jpg",
		"photos/dog.jpg",
		"photos2/x.jpg",
		"photos2/y.jpg",
		"videos/rec1.mp4",
		"videos/rec2.mp4",
		"unicode/日本語/test.txt",
		"unicode/español.txt",
		"z.txt",
	}

	sort.Strings(names)

	for i, name := range names {
		id, err := store.BeginUpload(ctx, name, "application/octet-stream", false)
		if err != nil {
			t.Fatalf("BeginUpload %s: %v", name, err)
		}
		_, err = store.CommitObject(ctx, metadata.CommitInput{
			ObjectID:       id,
			Name:           name,
			Size:           int64(100 + i),
			ChecksumSHA256: fmt.Sprintf("sha-%d", i),
			ChunkCount:     1,
			Chunks: []metadata.ChunkCommit{
				{ChunkID: id + "_0", Index: 0, Size: int64(100 + i), SHA256: fmt.Sprintf("sha-%d", i), Replicas: []string{"w1"}},
			},
		})
		if err != nil {
			t.Fatalf("CommitObject %s: %v", name, err)
		}
	}

	t.Run("ExactName lookup", func(t *testing.T) {
		res, err := store.List(ctx, metadata.ListParams{ExactName: "photos/cat.jpg"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(res.Objects) != 1 || res.Objects[0].Name != "photos/cat.jpg" {
			t.Fatalf("expected photos/cat.jpg, got: %+v", res.Objects)
		}

		resMissing, err := store.List(ctx, metadata.ListParams{ExactName: "nonexistent.jpg"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(resMissing.Objects) != 0 {
			t.Fatalf("expected empty list for nonexistent, got: %+v", resMissing.Objects)
		}
	})

	t.Run("No delimiter listing all objects", func(t *testing.T) {
		res, err := store.List(ctx, metadata.ListParams{Limit: 100})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		var gotNames []string
		for _, o := range res.Objects {
			gotNames = append(gotNames, o.Name)
		}
		if !reflect.DeepEqual(gotNames, names) {
			t.Errorf("expected %v, got %v", names, gotNames)
		}
		if len(res.CommonPrefixes) != 0 {
			t.Errorf("expected 0 common prefixes, got %v", res.CommonPrefixes)
		}
	})

	t.Run("Delimiter slash at root", func(t *testing.T) {
		res, err := store.List(ctx, metadata.ListParams{Delimiter: "/", Limit: 100})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		// Expect objects at root: a.txt, b.txt, z.txt
		// Common prefixes: docs/, photos/, photos2/, unicode/, videos/
		expectedObjs := []string{"a.txt", "b.txt", "z.txt"}
		expectedCPs := []string{"docs/", "photos/", "photos2/", "unicode/", "videos/"}

		var gotObjs []string
		for _, o := range res.Objects {
			gotObjs = append(gotObjs, o.Name)
		}
		sort.Strings(gotObjs)
		sort.Strings(expectedObjs)
		if !reflect.DeepEqual(gotObjs, expectedObjs) {
			t.Errorf("expected objects %v, got %v", expectedObjs, gotObjs)
		}

		gotCPs := res.CommonPrefixes
		sort.Strings(gotCPs)
		sort.Strings(expectedCPs)
		if !reflect.DeepEqual(gotCPs, expectedCPs) {
			t.Errorf("expected CPs %v, got %v", expectedCPs, gotCPs)
		}
	})

	t.Run("Prefix boundary photos/ must not match photos2/", func(t *testing.T) {
		res, err := store.List(ctx, metadata.ListParams{Prefix: "photos/", Delimiter: "/", Limit: 100})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		for _, o := range res.Objects {
			if o.Name == "photos2/x.jpg" || o.Name == "photos2/y.jpg" {
				t.Errorf("photos/ prefix incorrectly matched photos2 object: %s", o.Name)
			}
		}
		for _, cp := range res.CommonPrefixes {
			if cp == "photos2/" {
				t.Errorf("photos/ prefix incorrectly matched photos2 common prefix: %s", cp)
			}
		}
	})

	t.Run("Pagination with limit 2 walking entire set", func(t *testing.T) {
		cursor := ""
		seenObjects := make(map[string]bool)
		seenCPs := make(map[string]bool)
		pageCount := 0

		for {
			pageCount++
			if pageCount > 50 {
				t.Fatal("infinite loop detected in pagination")
			}
			res, err := store.List(ctx, metadata.ListParams{
				Delimiter: "/",
				Limit:     2,
				Cursor:    cursor,
			})
			if err != nil {
				t.Fatalf("page %d failed: %v", pageCount, err)
			}

			for _, o := range res.Objects {
				if seenObjects[o.Name] {
					t.Errorf("duplicate object seen: %s", o.Name)
				}
				seenObjects[o.Name] = true
			}

			for _, cp := range res.CommonPrefixes {
				if seenCPs[cp] {
					t.Errorf("duplicate CP seen: %s", cp)
				}
				seenCPs[cp] = true
			}

			if !res.Truncated {
				break
			}
			if res.NextCursor == "" {
				t.Fatalf("page %d was truncated but NextCursor is empty", pageCount)
			}
			cursor = res.NextCursor
		}

		expectedTotal := 3 + 5 // 3 root objects + 5 prefixes
		totalSeen := len(seenObjects) + len(seenCPs)
		if totalSeen != expectedTotal {
			t.Errorf("expected %d total items across pages, got %d (objs: %v, cps: %v)", expectedTotal, totalSeen, seenObjects, seenCPs)
		}
	})

	t.Run("Unicode prefix listing", func(t *testing.T) {
		res, err := store.List(ctx, metadata.ListParams{Prefix: "unicode/", Delimiter: "/"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(res.Objects) != 1 || res.Objects[0].Name != "unicode/español.txt" {
			t.Errorf("unexpected unicode objects: %+v", res.Objects)
		}
		if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0] != "unicode/日本語/" {
			t.Errorf("unexpected unicode CPs: %+v", res.CommonPrefixes)
		}
	})
}
