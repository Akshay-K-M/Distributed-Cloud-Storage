// This file exposes metadata queries used by reconciliation and repair.
package metadata

import (
	"context"
	"database/sql"
	"errors"
)

// ListChunksForReconciliation queries all chunks for COMMITTED objects along with their replica rows.
func (s *DBStore) ListChunksForReconciliation(ctx context.Context) ([]ChunkReplicaInfo, error) {
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT c.chunk_id, c.size, c.sha256, COALESCE(r.worker_id, ''), COALESCE(r.state, '')
		FROM chunks c
		JOIN objects o ON c.object_id = o.object_id
		LEFT JOIN chunk_replicas r ON c.chunk_id = r.chunk_id
		WHERE o.state = 'COMMITTED'
		ORDER BY c.chunk_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ChunkReplicaInfo
	var cur *ChunkReplicaInfo

	for rows.Next() {
		var chunkID, sha, workerID, state string
		var size int64
		if err := rows.Scan(&chunkID, &size, &sha, &workerID, &state); err != nil {
			return nil, err
		}

		if cur == nil || cur.ChunkID != chunkID {
			result = append(result, ChunkReplicaInfo{
				ChunkID:  chunkID,
				Size:     size,
				SHA256:   sha,
				Replicas: []ReplicaRow{},
			})
			cur = &result[len(result)-1]
		}

		if workerID != "" {
			cur.Replicas = append(cur.Replicas, ReplicaRow{
				WorkerID: workerID,
				State:    state,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// InsertReplicaIfCommitted conditionally inserts a replica record only if the owning object is COMMITTED.
func (s *DBStore) InsertReplicaIfCommitted(ctx context.Context, chunkID, workerID string, now int64) (bool, error) {
	res, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO chunk_replicas (chunk_id, worker_id, state, updated_at)
		SELECT ?, ?, 'OK', ?
		WHERE EXISTS (
			SELECT 1 FROM chunks c
			JOIN objects o ON c.object_id = o.object_id
			WHERE c.chunk_id = ? AND o.state = 'COMMITTED'
		)
		ON CONFLICT(chunk_id, worker_id) DO UPDATE SET
			state = 'OK',
			updated_at = excluded.updated_at
	`, chunkID, workerID, now, chunkID)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// ListDeletingObjects returns chunks and replicas for objects in DELETING state.
func (s *DBStore) ListDeletingObjects(ctx context.Context) ([]DeletingObjectChunk, error) {
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT o.object_id, c.chunk_id, COALESCE(r.worker_id, '')
		FROM objects o
		JOIN chunks c ON o.object_id = c.object_id
		LEFT JOIN chunk_replicas r ON c.chunk_id = r.chunk_id
		WHERE o.state = 'DELETING'
		ORDER BY o.object_id, c.chunk_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DeletingObjectChunk
	for rows.Next() {
		var doc DeletingObjectChunk
		if err := rows.Scan(&doc.ObjectID, &doc.ChunkID, &doc.WorkerID); err != nil {
			return nil, err
		}
		result = append(result, doc)
	}
	return result, rows.Err()
}

// DeleteObject deletes an object row, which cascades to delete all its chunks and replica records.
func (s *DBStore) DeleteObject(ctx context.Context, objectID string) error {
	_, err := s.writeDB.ExecContext(ctx, "DELETE FROM objects WHERE object_id = ?", objectID)
	return err
}

// GetChunkObjectInfo returns state and sha for a chunk, used by inventory sync.
func (s *DBStore) GetChunkObjectInfo(ctx context.Context, chunkID string) (string, string, string, error) {
	var state, sha, objID string
	err := s.readDB.QueryRowContext(ctx, `
		SELECT o.state, c.sha256, o.object_id
		FROM chunks c
		JOIN objects o ON c.object_id = o.object_id
		WHERE c.chunk_id = ?
	`, chunkID).Scan(&state, &sha, &objID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", ErrNotFound
	}
	return state, sha, objID, err
}

// ListChunkIDsForWorker returns all chunk IDs recorded in DB for workerID.
func (s *DBStore) ListChunkIDsForWorker(ctx context.Context, workerID string) ([]string, error) {
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT chunk_id FROM chunk_replicas WHERE worker_id = ?
	`, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
