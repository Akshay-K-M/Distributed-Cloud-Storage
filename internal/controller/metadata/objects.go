// This file implements object lifecycle and metadata reads in SQLite.
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/ids"
)

func (s *DBStore) BeginUpload(ctx context.Context, name, contentType string, overwrite bool) (string, error) {
	if !overwrite && name != "" {
		var exists int
		err := s.readDB.QueryRowContext(ctx, "SELECT 1 FROM objects WHERE name = ? AND state = 'COMMITTED' LIMIT 1", name).Scan(&exists)
		if err == nil {
			return "", ErrNameConflict
		} else if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
	}

	objectID, err := ids.NewObjectID()
	if err != nil {
		return "", err
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	now := time.Now().UnixMilli()
	_, err = s.writeDB.ExecContext(ctx, `
		INSERT INTO objects (object_id, name, content_type, state, created_at, modified_at, size, checksum_sha256, chunk_count, custom_metadata)
		VALUES (?, ?, ?, 'UPLOADING', ?, ?, 0, '', 0, '{}')
	`, objectID, name, contentType, now, now)
	if err != nil {
		return "", err
	}

	return objectID, nil
}

func (s *DBStore) CommitObject(ctx context.Context, c CommitInput) (*api.Object, error) {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UnixMilli()

	// (1) If Overwrite, mark existing committed object with the same name as DELETING
	if c.Overwrite {
		_, err = tx.ExecContext(ctx, `
			UPDATE objects SET state = 'DELETING', modified_at = ?
			WHERE name = ? AND state = 'COMMITTED'
		`, now, c.Name)
		if err != nil {
			return nil, err
		}
	}

	// (2) Insert all chunks
	stmtChunk, err := tx.PrepareContext(ctx, `
		INSERT INTO chunks (chunk_id, object_id, idx, size, sha256)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, err
	}
	defer stmtChunk.Close()

	// (3) Insert all chunk replicas
	stmtReplica, err := tx.PrepareContext(ctx, `
		INSERT INTO chunk_replicas (chunk_id, worker_id, state, updated_at)
		VALUES (?, ?, 'OK', ?)
	`)
	if err != nil {
		return nil, err
	}
	defer stmtReplica.Close()

	for _, ch := range c.Chunks {
		if _, err := stmtChunk.ExecContext(ctx, ch.ChunkID, c.ObjectID, ch.Index, ch.Size, ch.SHA256); err != nil {
			return nil, err
		}
		for _, wID := range ch.Replicas {
			if _, err := stmtReplica.ExecContext(ctx, ch.ChunkID, wID, now); err != nil {
				return nil, err
			}
		}
	}

	metaBytes := []byte("{}")
	if len(c.CustomMetadata) > 0 {
		metaBytes, err = json.Marshal(c.CustomMetadata)
		if err != nil {
			return nil, err
		}
	}

	contentType := c.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// (4) Update object state to COMMITTED
	res, err := tx.ExecContext(ctx, `
		UPDATE objects
		SET name = ?, state = 'COMMITTED', size = ?, checksum_sha256 = ?, chunk_count = ?, content_type = ?, custom_metadata = ?, modified_at = ?
		WHERE object_id = ? AND state = 'UPLOADING'
	`, c.Name, c.Size, c.ChecksumSHA256, c.ChunkCount, contentType, string(metaBytes), now, c.ObjectID)
	if err != nil {
		if isUniqueConstraintError(err) {
			return nil, ErrNameConflict
		}
		return nil, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, ErrNotFound
	}

	// Query created_at and name for the response object
	var createdAt int64
	var name string
	err = tx.QueryRowContext(ctx, "SELECT created_at, name FROM objects WHERE object_id = ?", c.ObjectID).Scan(&createdAt, &name)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		if isUniqueConstraintError(err) {
			return nil, ErrNameConflict
		}
		return nil, err
	}

	customMeta := c.CustomMetadata
	if customMeta == nil {
		customMeta = make(map[string]string)
	}

	return &api.Object{
		ObjectID:       c.ObjectID,
		Name:           name,
		Size:           c.Size,
		ContentType:    contentType,
		ChecksumSHA256: c.ChecksumSHA256,
		ChunkCount:     c.ChunkCount,
		CreatedAt:      formatTimestamp(createdAt),
		ModifiedAt:     formatTimestamp(now),
		CustomMetadata: customMeta,
	}, nil
}

func (s *DBStore) AbortUpload(ctx context.Context, objectID string) error {
	_, err := s.writeDB.ExecContext(ctx, "DELETE FROM objects WHERE object_id = ? AND state = 'UPLOADING'", objectID)
	return err
}

const committedObjectQuery = `
    SELECT object_id, name, size, content_type, checksum_sha256, chunk_count, created_at, modified_at, custom_metadata
    FROM objects WHERE object_id = ? AND state = 'COMMITTED'`

func (s *DBStore) GetObject(ctx context.Context, id string) (*api.Object, error) {
	return scanObject(s.readDB.QueryRowContext(ctx, committedObjectQuery, id))
}

func scanObject(row *sql.Row) (*api.Object, error) {
	var obj api.Object
	var createdAt, modifiedAt int64
	var metaJSON string
	err := row.Scan(
		&obj.ObjectID,
		&obj.Name,
		&obj.Size,
		&obj.ContentType,
		&obj.ChecksumSHA256,
		&obj.ChunkCount,
		&createdAt,
		&modifiedAt,
		&metaJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	obj.CreatedAt = formatTimestamp(createdAt)
	obj.ModifiedAt = formatTimestamp(modifiedAt)
	obj.CustomMetadata = make(map[string]string)
	if metaJSON != "" && metaJSON != "{}" {
		if err := json.Unmarshal([]byte(metaJSON), &obj.CustomMetadata); err != nil {
			return nil, err
		}
	}

	return &obj, nil
}

func (s *DBStore) GetObjectWithChunks(ctx context.Context, id string) (*api.ObjectDetail, error) {
	// Read the summary and locations from one SQLite snapshot even if a
	// concurrent request deletes the object or changes its metadata.
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return objectDetail(ctx, tx, id)
}

func objectDetail(ctx context.Context, tx *sql.Tx, id string) (*api.ObjectDetail, error) {
	obj, err := scanObject(tx.QueryRowContext(ctx, committedObjectQuery, id))
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT c.chunk_id, c.idx, c.size, c.sha256, COALESCE(r.worker_id, ''), COALESCE(r.state, '')
		FROM chunks c LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id
		WHERE c.object_id = ? ORDER BY c.idx, r.worker_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	chunks := make([]api.ChunkInfo, 0, obj.ChunkCount)
	for rows.Next() {
		var ch api.ChunkInfo
		var replica api.ReplicaInfo
		if err := rows.Scan(&ch.ChunkID, &ch.Index, &ch.Size, &ch.SHA256, &replica.WorkerID, &replica.State); err != nil {
			return nil, err
		}
		if len(chunks) == 0 || chunks[len(chunks)-1].ChunkID != ch.ChunkID {
			ch.Replicas = []api.ReplicaInfo{}
			chunks = append(chunks, ch)
		}
		if replica.WorkerID != "" {
			last := &chunks[len(chunks)-1]
			last.Replicas = append(last.Replicas, replica)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &api.ObjectDetail{Object: *obj, Chunks: chunks}, nil
}

func (s *DBStore) MarkDeleting(ctx context.Context, id string) error {
	now := time.Now().UnixMilli()
	res, err := s.writeDB.ExecContext(ctx, `
		UPDATE objects SET state = 'DELETING', modified_at = ?
		WHERE object_id = ? AND state = 'COMMITTED'
	`, now, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *DBStore) CleanupStaleUploading(ctx context.Context) (int, error) {
	res, err := s.writeDB.ExecContext(ctx, "DELETE FROM objects WHERE state = 'UPLOADING'")
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "constraint failed: 2067")
}
