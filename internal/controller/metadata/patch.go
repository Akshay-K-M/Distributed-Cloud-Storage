// This file applies validated custom-metadata patches transactionally.
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"objstore/internal/api"
)

var validKeyRegex = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func (s *DBStore) PatchMetadata(ctx context.Context, id string, patch map[string]*string) (*api.ObjectDetail, error) {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var metaJSON string
	err = tx.QueryRowContext(ctx, `
		SELECT custom_metadata FROM objects
		WHERE object_id = ? AND state = 'COMMITTED'
	`, id).Scan(&metaJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	current := make(map[string]string)
	if metaJSON != "" && metaJSON != "{}" {
		if err := json.Unmarshal([]byte(metaJSON), &current); err != nil {
			return nil, err
		}
	}

	for k, v := range patch {
		if v == nil {
			delete(current, k)
		} else {
			if !validKeyRegex.MatchString(k) {
				return nil, ErrInvalidRequest
			}
			if len(*v) > 1024 {
				return nil, ErrInvalidRequest
			}
			current[k] = *v
		}
	}

	if len(current) > 64 {
		return nil, ErrInvalidRequest
	}

	newBytes, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `
		UPDATE objects SET custom_metadata = ?, modified_at = ?
		WHERE object_id = ? AND state = 'COMMITTED'
	`, string(newBytes), now, id)
	if err != nil {
		return nil, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, ErrNotFound
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return s.GetObjectWithChunks(ctx, id)
}
