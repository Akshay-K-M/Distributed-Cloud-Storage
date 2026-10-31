// This file implements object listing, prefix grouping, and pagination.
package metadata

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"strings"

	"objstore/internal/api"
)

func (s *DBStore) List(ctx context.Context, params ListParams) (*api.ListResponse, error) {
	// Fast path: exact name lookup
	if params.ExactName != "" {
		var lo api.ListObject
		var createdAt int64
		err := s.readDB.QueryRowContext(ctx, `
			SELECT object_id, name, size, content_type, created_at
			FROM objects
			WHERE state = 'COMMITTED' AND name = ?
		`, params.ExactName).Scan(
			&lo.ObjectID,
			&lo.Name,
			&lo.Size,
			&lo.ContentType,
			&createdAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return &api.ListResponse{
				Objects:        []api.ListObject{},
				CommonPrefixes: []string{},
				NextCursor:     "",
				Truncated:      false,
			}, nil
		}
		if err != nil {
			return nil, err
		}
		lo.CreatedAt = formatTimestamp(createdAt)
		return &api.ListResponse{
			Objects:        []api.ListObject{lo},
			CommonPrefixes: []string{},
			NextCursor:     "",
			Truncated:      false,
		}, nil
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	start := params.Prefix
	if params.Cursor != "" {
		dec, err := base64.StdEncoding.DecodeString(params.Cursor)
		if err != nil {
			return nil, ErrInvalidRequest
		}
		start = string(dec)
	}

	upper := upperPrefix(params.Prefix)

	objects := make([]api.ListObject, 0)
	commonPrefixes := make([]string, 0)
	cpSet := make(map[string]bool)

	for (len(objects) + len(commonPrefixes)) < limit {
		remaining := limit - (len(objects) + len(commonPrefixes))
		var query string
		var args []any

		if upper != "" {
			query = `
				SELECT name, object_id, size, content_type, created_at
				FROM objects
				WHERE state = 'COMMITTED' AND name >= ? AND name < ?
				ORDER BY name ASC
				LIMIT ?
			`
			args = []any{start, upper, remaining + 1}
		} else {
			query = `
				SELECT name, object_id, size, content_type, created_at
				FROM objects
				WHERE state = 'COMMITTED' AND name >= ?
				ORDER BY name ASC
				LIMIT ?
			`
			args = []any{start, remaining + 1}
		}

		rows, err := s.readDB.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}

		rowCount := 0
		brokenForCP := false

		for rows.Next() {
			rowCount++
			var name, objID, contentType string
			var size, createdAt int64
			if err := rows.Scan(&name, &objID, &size, &contentType, &createdAt); err != nil {
				rows.Close()
				return nil, err
			}

			if !strings.HasPrefix(name, params.Prefix) {
				continue
			}

			rest := name[len(params.Prefix):]
			if params.Delimiter != "" && strings.Contains(rest, params.Delimiter) {
				idx := strings.Index(rest, params.Delimiter)
				cp := params.Prefix + rest[:idx+len(params.Delimiter)]
				if !cpSet[cp] {
					cpSet[cp] = true
					commonPrefixes = append(commonPrefixes, cp)
				}
				start = nextAfterPrefix(cp)
				brokenForCP = true
				break
			}

			objects = append(objects, api.ListObject{
				ObjectID:    objID,
				Name:        name,
				Size:        size,
				ContentType: contentType,
				CreatedAt:   formatTimestamp(createdAt),
			})
			start = name + "\x00"

			if (len(objects) + len(commonPrefixes)) == limit {
				break
			}
		}
		rows.Close()

		if rowCount == 0 {
			break
		}
		if !brokenForCP && (len(objects)+len(commonPrefixes)) < limit && rowCount <= remaining {
			// No more rows available
			break
		}
	}

	truncated := false
	nextCursor := ""

	if (len(objects) + len(commonPrefixes)) == limit {
		var dummy int
		var checkQuery string
		var checkArgs []any

		if upper != "" {
			checkQuery = `
				SELECT 1 FROM objects
				WHERE state = 'COMMITTED' AND name >= ? AND name < ?
				LIMIT 1
			`
			checkArgs = []any{start, upper}
		} else {
			checkQuery = `
				SELECT 1 FROM objects
				WHERE state = 'COMMITTED' AND name >= ?
				LIMIT 1
			`
			checkArgs = []any{start}
		}

		err := s.readDB.QueryRowContext(ctx, checkQuery, checkArgs...).Scan(&dummy)
		if err == nil {
			truncated = true
			nextCursor = base64.StdEncoding.EncodeToString([]byte(start))
		}
	}

	return &api.ListResponse{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		NextCursor:     nextCursor,
		Truncated:      truncated,
	}, nil
}

func upperPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 255 {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

func nextAfterPrefix(cp string) string {
	b := []byte(cp)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 255 {
			b[i]++
			return string(b[:i+1])
		}
	}
	return cp + "\x00"
}
