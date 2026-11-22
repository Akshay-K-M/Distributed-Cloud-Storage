// This file persists worker, chunk, and replica state for the controller.
package metadata

import (
	"context"

	"objstore/internal/api"
)

func (s *DBStore) UpsertWorker(ctx context.Context, w WorkerRow) error {
	_, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO workers (worker_id, address, status, capacity_bytes, used_bytes, chunk_count, registered_at, last_status_change)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET
			address = excluded.address,
			status = excluded.status,
			capacity_bytes = excluded.capacity_bytes,
			used_bytes = excluded.used_bytes,
			chunk_count = excluded.chunk_count,
			last_status_change = excluded.last_status_change
	`, w.WorkerID, w.Address, w.Status, w.CapacityBytes, w.UsedBytes, w.ChunkCount, w.RegisteredAt, w.LastStatusChange)
	return err
}

func (s *DBStore) SetWorkerStatus(ctx context.Context, workerID string, status string, now int64) error {
	_, err := s.writeDB.ExecContext(ctx, `
		UPDATE workers SET status = ?, last_status_change = ?
		WHERE worker_id = ?
	`, status, now, workerID)
	return err
}

func (s *DBStore) ListWorkers(ctx context.Context) ([]WorkerRow, error) {
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT worker_id, address, status, capacity_bytes, used_bytes, chunk_count, registered_at, last_status_change
		FROM workers
		ORDER BY worker_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []WorkerRow
	for rows.Next() {
		var w WorkerRow
		if err := rows.Scan(&w.WorkerID, &w.Address, &w.Status, &w.CapacityBytes, &w.UsedBytes, &w.ChunkCount, &w.RegisteredAt, &w.LastStatusChange); err != nil {
			return nil, err
		}
		result = append(result, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		result = []WorkerRow{}
	}
	return result, nil
}

// Inventory reports cached sidecar hashes. Only a verified repair may clear a
// corruption finding made by reading the actual bytes.
func (s *DBStore) UpsertReplica(ctx context.Context, chunkID, workerID, state string, updatedAt int64) error {
	_, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO chunk_replicas (chunk_id, worker_id, state, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(chunk_id, worker_id) DO UPDATE SET
			state = excluded.state,
			updated_at = excluded.updated_at
        WHERE chunk_replicas.state != 'CORRUPT' OR excluded.state = 'CORRUPT'
	`, chunkID, workerID, state, updatedAt)
	return err
}

func (s *DBStore) SetReplicaState(ctx context.Context, chunkID, workerID, state string, updatedAt int64) error {
	_, err := s.writeDB.ExecContext(ctx, `
		UPDATE chunk_replicas SET state = ?, updated_at = ?
		WHERE chunk_id = ? AND worker_id = ?
	`, state, updatedAt, chunkID, workerID)
	return err
}

func (s *DBStore) DeleteReplica(ctx context.Context, chunkID, workerID string) error {
	_, err := s.writeDB.ExecContext(ctx, `
		DELETE FROM chunk_replicas
		WHERE chunk_id = ? AND worker_id = ?
	`, chunkID, workerID)
	return err
}

func (s *DBStore) ObjectCounts(ctx context.Context) (api.ObjectCounts, error) {
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT state, count(*) FROM objects GROUP BY state
	`)
	if err != nil {
		return api.ObjectCounts{}, err
	}
	defer rows.Close()

	var counts api.ObjectCounts
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return counts, err
		}
		switch state {
		case "COMMITTED":
			counts.Committed = count
		case "UPLOADING":
			counts.Uploading = count
		case "DELETING":
			counts.Deleting = count
		}
	}
	return counts, rows.Err()
}

func (s *DBStore) ChunkCounts(ctx context.Context) (api.ChunkCounts, error) {
	var counts api.ChunkCounts

	// Total chunks
	if err := s.readDB.QueryRowContext(ctx, "SELECT count(*) FROM chunks").Scan(&counts.Total); err != nil {
		return counts, err
	}

	// Corrupt replicas
	if err := s.readDB.QueryRowContext(ctx, "SELECT count(*) FROM chunk_replicas WHERE state = 'CORRUPT'").Scan(&counts.CorruptReplicas); err != nil {
		return counts, err
	}

	// Lost chunks (no OK replica)
	err := s.readDB.QueryRowContext(ctx, `
		SELECT count(*) FROM chunks c
		WHERE NOT EXISTS (
			SELECT 1 FROM chunk_replicas r
			WHERE r.chunk_id = c.chunk_id AND r.state = 'OK'
		)
	`).Scan(&counts.Lost)
	if err != nil {
		return counts, err
	}

	// Under- and over-replication are computed by the reconciler.
	counts.UnderReplicated = 0
	counts.OverReplicated = 0

	return counts, nil
}
