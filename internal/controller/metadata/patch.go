// This file applies validated custom-metadata patches transactionally.
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"objstore/internal/api"
)

var validKeyRegex = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// ValidateCustomMetadata applies the same limits at upload and after a patch.
func ValidateCustomMetadata(labels map[string]string) error {
	if len(labels) > 64 {
		return fmt.Errorf("%w: maximum 64 metadata keys", ErrInvalidRequest)
	}
	for key, value := range labels {
		if !validKeyRegex.MatchString(key) {
			return fmt.Errorf("%w: invalid metadata key", ErrInvalidRequest)
		}
		if len(value) > 1024 {
			return fmt.Errorf("%w: metadata value exceeds 1024 bytes", ErrInvalidRequest)
		}
	}
	return nil
}

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
			current[k] = *v
		}
	}

	if err := ValidateCustomMetadata(current); err != nil {
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

	updated, err := objectDetail(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return updated, nil
}
