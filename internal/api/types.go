package api

// Frozen DTOs shared by every component (plan section 5).

// Object is the public object summary returned on upload and metadata GET.
type Object struct {
	ObjectID       string            `json:"object_id"`
	Name           string            `json:"name"`
	Size           int64             `json:"size"`
	ContentType    string            `json:"content_type"`
	ChecksumSHA256 string            `json:"checksum_sha256"`
	ChunkCount     int               `json:"chunk_count"`
	CreatedAt      string            `json:"created_at"`
	ModifiedAt     string            `json:"modified_at"`
	CustomMetadata map[string]string `json:"custom_metadata"`
}

// ObjectDetail is GET/PATCH /metadata.
type ObjectDetail struct {
	Object
	Chunks []ChunkInfo `json:"chunks"`
}

// ChunkInfo is one chunk in object metadata.
type ChunkInfo struct {
	Index    int           `json:"index"`
	ChunkID  string        `json:"chunk_id"`
	Size     int64         `json:"size"`
	SHA256   string        `json:"sha256"`
	Replicas []ReplicaInfo `json:"replicas"`
}

// ReplicaInfo is a replica location/state pair.
type ReplicaInfo struct {
	WorkerID string `json:"worker_id"`
	State    string `json:"state"`
}

// ListResponse is GET /api/v1/objects.
type ListResponse struct {
	Objects        []ListObject `json:"objects"`
	CommonPrefixes []string     `json:"common_prefixes"`
	NextCursor     string       `json:"next_cursor"`
	Truncated      bool         `json:"truncated"`
}

// ListObject is a row in a listing.
type ListObject struct {
	ObjectID    string `json:"object_id"`
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	CreatedAt   string `json:"created_at"`
}

// AdminStatus is GET /api/v1/admin/status.
type AdminStatus struct {
	Config  AdminConfig  `json:"config"`
	Workers []WorkerInfo `json:"workers"`
	Objects ObjectCounts `json:"objects"`
	Chunks  ChunkCounts  `json:"chunks"`
}

// AdminConfig is the cluster policy snapshot.
type AdminConfig struct {
	ReplicationFactor int   `json:"replication_factor"`
	WriteAcks         int   `json:"write_acks"`
	ChunkSize         int64 `json:"chunk_size"`
}

// WorkerInfo is one worker in admin status.
type WorkerInfo struct {
	WorkerID      string `json:"worker_id"`
	Status        string `json:"status"`
	Address       string `json:"address"`
	UsedBytes     int64  `json:"used_bytes"`
	ChunkCount    int    `json:"chunk_count"`
	LastSeenMSAgo int64  `json:"last_seen_ms_ago"`
}

// ObjectCounts is object-state tallies.
type ObjectCounts struct {
	Committed int `json:"committed"`
	Uploading int `json:"uploading"`
	Deleting  int `json:"deleting"`
}

// ChunkCounts is replica-health tallies.
type ChunkCounts struct {
	Total           int `json:"total"`
	UnderReplicated int `json:"under_replicated"`
	OverReplicated  int `json:"over_replicated"`
	CorruptReplicas int `json:"corrupt_replicas"`
	Lost            int `json:"lost"`
}

// ChunkPutResponse is PUT /internal/chunks/{id}.
type ChunkPutResponse struct {
	ChunkID string `json:"chunk_id"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// Inventory is GET /internal/workers/{id}/chunks.
type Inventory struct {
	WorkerID    string            `json:"worker_id"`
	Chunks      []InventoryChunk  `json:"chunks"`
	Quarantined []string          `json:"quarantined"`
}

// InventoryChunk is one live chunk in a worker inventory.
type InventoryChunk struct {
	ChunkID string `json:"chunk_id"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// RegisterRequest is POST /internal/workers/register.
type RegisterRequest struct {
	WorkerID      string `json:"worker_id"`
	Address       string `json:"address"`
	CapacityBytes int64  `json:"capacity_bytes"`
	UsedBytes     int64  `json:"used_bytes"`
	ChunkCount    int    `json:"chunk_count"`
}

// RegisterResponse is the register reply.
type RegisterResponse struct {
	HeartbeatIntervalMS int64 `json:"heartbeat_interval_ms"`
}

// HeartbeatRequest is POST /internal/workers/heartbeat.
type HeartbeatRequest struct {
	WorkerID      string   `json:"worker_id"`
	UsedBytes     int64    `json:"used_bytes"`
	CapacityBytes int64    `json:"capacity_bytes"`
	ChunkCount    int      `json:"chunk_count"`
	CorruptChunks []string `json:"corrupt_chunks"`
}

// Healthz is GET /healthz.
type Healthz struct {
	Status   string `json:"status"`
	WorkerID string `json:"worker_id,omitempty"`
}
