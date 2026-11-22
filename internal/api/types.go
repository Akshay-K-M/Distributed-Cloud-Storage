// This file contains JSON DTOs shared by clients, controllers, and workers.
// DTO means "data transfer object": a structure describing a request or response.
// These types carry data; server/client functions elsewhere perform the work.
// Sharing them keeps all components speaking the same JSON format. File bytes
// travel separately as binary streams, not as JSON fields in these structures.
package api

// Object is the public object summary returned on upload and metadata GET.
// It describes the whole file: its generated ID, name, byte size, content type,
// integrity checksum, chunk count, timestamps, and user-defined labels.
// Example: uploading obj:photos/cat.jpg returns this summary after commit.
// JSON tags select API field names: ObjectID is sent as "object_id".
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
// Extends the object summary with the chunk layout. This is what ocs stat uses
// to show how the original file is split and which workers hold its replicas.
// The embedded Object contributes its fields directly to the same JSON object.
type ObjectDetail struct {
	Object
	Chunks []ChunkInfo `json:"chunks"`
}

// ChunkInfo is one chunk in object metadata.
// Index preserves reconstruction order: 0, then 1, then 2, etc. SHA256 checks
// this chunk's bytes; Replicas lists copies of this same chunk, not other chunks.
type ChunkInfo struct {
	Index    int           `json:"index"`
	ChunkID  string        `json:"chunk_id"`
	Size     int64         `json:"size"`
	SHA256   string        `json:"sha256"`
	Replicas []ReplicaInfo `json:"replicas"`
}

// ReplicaInfo is a replica location/state pair.
// Example: {"worker_id":"worker1","state":"OK"}. State records replica
// integrity (OK/CORRUPT); worker availability (ALIVE/DEAD) is tracked separately.
// Therefore an OK replica can temporarily belong to an unreachable worker.
type ReplicaInfo struct {
	WorkerID string `json:"worker_id"`
	State    string `json:"state"`
}

// ListResponse is GET /api/v1/objects.
// Carries one listing page: object summaries and folder-like grouped prefixes.
// Example: listing photos/ may return photos/cat.jpg and a prefix photos/trips/.
// Truncated means more results exist; NextCursor tells the next request where
// to continue. The CLI's List function follows these pages automatically.
type ListResponse struct {
	Objects        []ListObject `json:"objects"`
	CommonPrefixes []string     `json:"common_prefixes"`
	NextCursor     string       `json:"next_cursor"`
	Truncated      bool         `json:"truncated"`
}

// ListObject is a row in a listing.
// A smaller summary keeps listing responses lighter than full object metadata:
// chunk locations and custom labels are fetched only when a caller needs detail.
type ListObject struct {
	ObjectID    string `json:"object_id"`
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	CreatedAt   string `json:"created_at"`
}

// AdminStatus is GET /api/v1/admin/status.
// Bundles settings, worker liveness, object lifecycle counts, and chunk health
// into the response displayed by ocs status. This is an operational overview.
type AdminStatus struct {
	Config  AdminConfig  `json:"config"`
	Workers []WorkerInfo `json:"workers"`
	Objects ObjectCounts `json:"objects"`
	Chunks  ChunkCounts  `json:"chunks"`
}

// AdminConfig is the cluster policy snapshot.
// ReplicationFactor is the desired copies per chunk; WriteAcks is the required
// successful replica writes before upload commit. Both default to 2 here.
// ChunkSize is in bytes: 1048576 means 1 MiB.
type AdminConfig struct {
	ReplicationFactor int   `json:"replication_factor"`
	WriteAcks         int   `json:"write_acks"`
	ChunkSize         int64 `json:"chunk_size"`
}

// WorkerInfo is one worker in admin status.
// Status is the controller's liveness assessment based on heartbeats. Storage
// statistics are last reported values, so a DEAD worker can still show used
// bytes and chunks. LastSeenMSAgo indicates how old its last contact is.
type WorkerInfo struct {
	WorkerID      string `json:"worker_id"`
	Status        string `json:"status"`
	Address       string `json:"address"`
	UsedBytes     int64  `json:"used_bytes"`
	ChunkCount    int    `json:"chunk_count"`
	LastSeenMSAgo int64  `json:"last_seen_ms_ago"`
}

// ObjectCounts is object-state tallies.
// Uploading means not yet committed; Committed means available to clients;
// Deleting means logically deleted with physical cleanup handled in background.
type ObjectCounts struct {
	Committed int `json:"committed"`
	Uploading int `json:"uploading"`
	Deleting  int `json:"deleting"`
}

// ChunkCounts is replica-health tallies.
// Under/OverReplicated count chunks below/above policy; CorruptReplicas counts
// damaged copies. Lost counts chunks the planner classifies as having no usable
// replica. These are computed assessments, not a direct scan of every disk.
type ChunkCounts struct {
	Total           int `json:"total"`
	UnderReplicated int `json:"under_replicated"`
	OverReplicated  int `json:"over_replicated"`
	CorruptReplicas int `json:"corrupt_replicas"`
	Lost            int `json:"lost"`
}

// ChunkPutResponse is PUT /internal/chunks/{id}.
// A worker acknowledges a stored chunk with its ID, size, and checksum. The
// controller uses worker acknowledgements while deciding whether to commit.
type ChunkPutResponse struct {
	ChunkID string `json:"chunk_id"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// Inventory is GET /internal/workers/{id}/chunks.
// A worker reports its live chunk records and quarantined chunk IDs. The
// reconciler compares this report with metadata to discover missing, corrupt,
// and orphaned replicas instead of relying only on past upload acknowledgements.
type Inventory struct {
	WorkerID    string           `json:"worker_id"`
	Chunks      []InventoryChunk `json:"chunks"`
	Quarantined []string         `json:"quarantined"`
}

// InventoryChunk is one live chunk in a worker inventory.
// Contains enough information to compare a stored copy with the controller's
// expected chunk record without transferring the chunk's binary contents.
type InventoryChunk struct {
	ChunkID string `json:"chunk_id"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

// RegisterRequest is POST /internal/workers/register.
// A worker introduces itself with its stable ID, reachable address, capacity,
// and current storage usage. The controller needs these for tracking/placement.
type RegisterRequest struct {
	WorkerID      string `json:"worker_id"`
	Address       string `json:"address"`
	CapacityBytes int64  `json:"capacity_bytes"`
	UsedBytes     int64  `json:"used_bytes"`
	ChunkCount    int    `json:"chunk_count"`
}

// RegisterResponse is the register reply.
// Tells the worker how frequently to send heartbeats, in milliseconds.
// Example: 2000 means report every two seconds.
type RegisterResponse struct {
	HeartbeatIntervalMS int64 `json:"heartbeat_interval_ms"`
}

// HeartbeatRequest is POST /internal/workers/heartbeat.
// Periodic "I'm here" report with updated usage and detected corrupt chunk IDs.
// Missing reports drive ALIVE -> SUSPECT -> DEAD; corruption reports allow the
// controller to mark affected replicas and arrange repair.
type HeartbeatRequest struct {
	WorkerID      string   `json:"worker_id"`
	UsedBytes     int64    `json:"used_bytes"`
	CapacityBytes int64    `json:"capacity_bytes"`
	ChunkCount    int      `json:"chunk_count"`
	CorruptChunks []string `json:"corrupt_chunks"`
}

// Healthz is GET /healthz.
// Small liveness response used by Docker health checks. A worker may include
// its ID; omitempty leaves that field out when empty (as on the controller).
// A healthy HTTP endpoint alone does not certify the whole cluster's data health.
type Healthz struct {
	Status   string `json:"status"`
	WorkerID string `json:"worker_id,omitempty"`
}
