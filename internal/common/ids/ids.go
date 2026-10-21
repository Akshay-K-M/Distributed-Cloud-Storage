package ids

import (
	"fmt"

	"github.com/google/uuid"
)

// NewObjectID returns a UUIDv7 object id.
func NewObjectID() (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// ChunkID is <object_id>_<idx>.
func ChunkID(objectID string, idx int) string {
	return fmt.Sprintf("%s_%d", objectID, idx)
}
