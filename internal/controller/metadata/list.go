// Object names form a flat, case-sensitive namespace. A delimiter groups names
// for the CLI without creating directories in the catalog.
package metadata

import (
	"context"
	"encoding/base64"
	"strings"

	"objstore/internal/api"
)

func (s *DBStore) List(ctx context.Context, params ListParams) (*api.ListResponse, error) {
	result := &api.ListResponse{Objects: []api.ListObject{}, CommonPrefixes: []string{}}
	query := "SELECT object_id, name, size, content_type, created_at FROM objects WHERE state = 'COMMITTED'"
	var args []any
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)

	if params.ExactName != "" {
		query += " AND name = ?"
		args = append(args, params.ExactName)
	} else {
		start := params.Prefix
		if params.Cursor != "" {
			decoded, err := base64.StdEncoding.DecodeString(params.Cursor)
			if err != nil {
				return nil, ErrInvalidRequest
			}
			start = max(start, string(decoded))
		}
		query += " AND name >= ? ORDER BY name"
		args = append(args, start)
	}
	rows, err := s.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lastGroup := ""
	for rows.Next() {
		var obj api.ListObject
		var createdAt int64
		if err := rows.Scan(&obj.ObjectID, &obj.Name, &obj.Size, &obj.ContentType, &createdAt); err != nil {
			return nil, err
		}
		group := ""
		if params.ExactName == "" {
			rest, matches := strings.CutPrefix(obj.Name, params.Prefix)
			if !matches {
				break
			} // Sorted names with this prefix are contiguous.
			if params.Delimiter != "" {
				if head, _, found := strings.Cut(rest, params.Delimiter); found {
					group = params.Prefix + head + params.Delimiter
				}
			}
		}
		if group != "" && group == lastGroup {
			continue
		}
		if len(result.Objects)+len(result.CommonPrefixes) == limit {
			// Resume at the first entry that didn't fit, including a whole group.
			result.Truncated = true
			result.NextCursor = base64.StdEncoding.EncodeToString([]byte(obj.Name))
			break
		}
		lastGroup = group
		if group != "" {
			result.CommonPrefixes = append(result.CommonPrefixes, group)
		} else {
			obj.CreatedAt = formatTimestamp(createdAt)
			result.Objects = append(result.Objects, obj)
		}
	}
	return result, rows.Err()
}
