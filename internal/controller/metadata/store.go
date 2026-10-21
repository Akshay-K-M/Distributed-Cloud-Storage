package metadata

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
	"objstore/internal/api"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

var (
	ErrNotFound      = errors.New("not found")
	ErrNameConflict  = errors.New("name conflict")
	ErrInvalidRequest = errors.New("invalid request")
)

// WorkerRow represents a persisted worker record.
type WorkerRow struct {
	WorkerID         string
	Address          string
	Status           string // ALIVE, SUSPECT, DEAD
	CapacityBytes    int64
	UsedBytes        int64
	ChunkCount       int
	RegisteredAt     int64 // unix ms
	LastStatusChange int64 // unix ms
}

// ChunkCommit contains the metadata of a chunk being committed.
type ChunkCommit struct {
	ChunkID  string
	Index    int
	Size     int64
	SHA256   string
	Replicas []string // worker IDs
}

// CommitInput is the input payload for CommitObject.
type CommitInput struct {
	ObjectID       string
	Name           string
	Size           int64
	ChecksumSHA256 string
	ChunkCount     int
	ContentType    string
	CustomMetadata map[string]string
	Overwrite      bool
	Chunks         []ChunkCommit
}

// ListParams specifies listing filters and pagination.
type ListParams struct {
	Prefix    string
	Delimiter string
	Limit     int
	Cursor    string
	ExactName string
}

// Store is the interface for controller metadata operations (plan section 2.1).
type Store interface {
	BeginUpload(ctx context.Context, name, contentType string, overwrite bool) (objectID string, err error)
	CommitObject(ctx context.Context, c CommitInput) (*api.Object, error)
	AbortUpload(ctx context.Context, objectID string) error
	GetObject(ctx context.Context, id string) (*api.Object, error)
	GetObjectWithChunks(ctx context.Context, id string) (*api.ObjectDetail, error)
	List(ctx context.Context, params ListParams) (*api.ListResponse, error)
	PatchMetadata(ctx context.Context, id string, patch map[string]*string) (*api.ObjectDetail, error)
	MarkDeleting(ctx context.Context, id string) error

	// Reconciler / registry helpers
	UpsertWorker(ctx context.Context, w WorkerRow) error
	SetWorkerStatus(ctx context.Context, workerID string, status string, now int64) error
	ListWorkers(ctx context.Context) ([]WorkerRow, error)
	UpsertReplica(ctx context.Context, chunkID, workerID, state string, updatedAt int64) error
	SetReplicaState(ctx context.Context, chunkID, workerID, state string, updatedAt int64) error
	DeleteReplica(ctx context.Context, chunkID, workerID string) error
	DeleteReplicasForWorker(ctx context.Context, workerID string) error
	CleanupStaleUploading(ctx context.Context) (int, error)

	// Admin stats helpers
	ObjectCounts(ctx context.Context) (api.ObjectCounts, error)
	ChunkCounts(ctx context.Context) (api.ChunkCounts, error)

	Close() error
}

// DBStore implements Store using SQLite with two connection pools (writeDB, readDB).
type DBStore struct {
	writeDB *sql.DB
	readDB  *sql.DB
}

// Open initializes SQLite databases for write and read handles with WAL and pragmas.
func Open(dbPath string) (*DBStore, error) {
	var writeDSN, readDSN string

	pragmas := "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"

	if dbPath == ":memory:" {
		// Use shared in-memory database URI so writeDB and readDB access the same data
		memURI := "file:mem_objstore?mode=memory&cache=shared"
		writeDSN = memURI + "&" + pragmas
		readDSN = memURI + "&" + pragmas + "&_pragma=query_only(1)"
	} else {
		dir := filepath.Dir(dbPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("create db dir: %w", err)
			}
		}
		sep := "?"
		if strings.Contains(dbPath, "?") {
			sep = "&"
		}
		writeDSN = dbPath + sep + pragmas
		readDSN = dbPath + sep + pragmas + "&_pragma=query_only(1)"
	}

	wDB, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("open write db: %w", err)
	}
	wDB.SetMaxOpenConns(1)

	// Explicitly apply pragmas to write handle
	if _, err := wDB.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=1; PRAGMA synchronous=FULL;"); err != nil {
		wDB.Close()
		return nil, fmt.Errorf("set write pragmas: %w", err)
	}

	// Run migrations on write handle
	if err := migrate(wDB); err != nil {
		wDB.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	rDB, err := sql.Open("sqlite", readDSN)
	if err != nil {
		wDB.Close()
		return nil, fmt.Errorf("open read db: %w", err)
	}
	rDB.SetMaxOpenConns(8)

	// Explicitly apply pragmas to read handle
	if _, err := rDB.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=1; PRAGMA synchronous=FULL; PRAGMA query_only=1;"); err != nil {
		// In case query_only fails on in-memory shared database before writes, ignore query_only error
		if _, err2 := rDB.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=1; PRAGMA synchronous=FULL;"); err2 != nil {
			wDB.Close()
			rDB.Close()
			return nil, fmt.Errorf("set read pragmas: %w", err2)
		}
	}

	return &DBStore{writeDB: wDB, readDB: rDB}, nil
}

func migrate(db *sql.DB) error {
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}

	if version < 1 {
		sqlBytes, err := migrationsFS.ReadFile("migrations/001_init.sql")
		if err != nil {
			return fmt.Errorf("read migration 001: %w", err)
		}
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if _, err := tx.Exec(string(sqlBytes)); err != nil {
			return fmt.Errorf("exec migration 001: %w", err)
		}
		if _, err := tx.Exec("PRAGMA user_version = 1"); err != nil {
			return fmt.Errorf("set user_version: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (s *DBStore) Close() error {
	var errs []error
	if s.readDB != nil {
		if err := s.readDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.writeDB != nil {
		if err := s.writeDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func formatTimestamp(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}
