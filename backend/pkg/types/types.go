// Package types defines the core domain types for the FT-DOSS system.
package types

import (
	"time"
)

// ─── Node States ──────────────────────────────────────────────────────────────

// NodeState represents the health state of a storage node.
type NodeState string

const (
	NodeStateHealthy        NodeState = "HEALTHY"
	NodeStateSuspect        NodeState = "SUSPECT"
	NodeStateUnavailable    NodeState = "UNAVAILABLE"
	NodeStateDecommissioning NodeState = "DECOMMISSIONING"
	NodeStateRecovering     NodeState = "RECOVERING"
)

// ─── Replica States ───────────────────────────────────────────────────────────

// ReplicaState represents the state of a single object replica on a storage node.
type ReplicaState string

const (
	ReplicaStateMissing   ReplicaState = "MISSING"
	ReplicaStatePresent   ReplicaState = "PRESENT"
	ReplicaStateStale     ReplicaState = "STALE"
	ReplicaStateCorrupt   ReplicaState = "CORRUPT"
	ReplicaStateRepairing ReplicaState = "REPAIRING"
	ReplicaStateValid     ReplicaState = "VALID"
)

// ─── Object States ────────────────────────────────────────────────────────────

// ObjectState represents the lifecycle state of an object version.
type ObjectState string

const (
	ObjectStatePreparing         ObjectState = "PREPARING"
	ObjectStateDataCommitted     ObjectState = "DATA_COMMITTED"
	ObjectStateMetadataCommitted ObjectState = "METADATA_COMMITTED"
	ObjectStateVisible           ObjectState = "VISIBLE"
	ObjectStateDeleted           ObjectState = "DELETED"
	ObjectStateTombstone         ObjectState = "TOMBSTONE"
)

// ─── Raft States ──────────────────────────────────────────────────────────────

// RaftState represents the role of a metadata node in the Raft cluster.
type RaftState string

const (
	RaftStateFollower  RaftState = "FOLLOWER"
	RaftStateCandidate RaftState = "CANDIDATE"
	RaftStateLeader    RaftState = "LEADER"
)

// ─── Checksum Algorithms ──────────────────────────────────────────────────────

// ChecksumAlgorithm specifies the algorithm used for integrity verification.
type ChecksumAlgorithm string

const (
	ChecksumSHA256 ChecksumAlgorithm = "sha256"
	ChecksumBLAKE3 ChecksumAlgorithm = "blake3"
	ChecksumMD5    ChecksumAlgorithm = "md5"
)

// ─── Storage Node ─────────────────────────────────────────────────────────────

// StorageNode represents a storage node in the cluster.
type StorageNode struct {
	ID           string    `json:"id"`
	Address      string    `json:"address"`
	GRPCPort     int       `json:"grpc_port"`
	HTTPPort     int       `json:"http_port"`
	Zone         string    `json:"zone"`
	Rack         string    `json:"rack"`
	Region       string    `json:"region"`
	State        NodeState `json:"state"`
	ClusterEpoch int64     `json:"cluster_epoch"`
	LastAppliedLog int64   `json:"last_applied_log"`
	DiskTotal    int64     `json:"disk_total"`
	DiskFree     int64     `json:"disk_free"`
	DiskUsed     int64     `json:"disk_used"`
	ObjectCount  int64     `json:"object_count"`
	ReplicaCount int64     `json:"replica_count"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	JoinedAt     time.Time `json:"joined_at"`
	Weight       float64   `json:"weight"`
}

// ─── Bucket ───────────────────────────────────────────────────────────────────

// Bucket represents a logical container for objects.
type Bucket struct {
	Name             string    `json:"name"`
	CreatedAt        time.Time `json:"created_at"`
	Owner            string    `json:"owner"`
	ReplicationFactor int      `json:"replication_factor"`
	WriteQuorum      int       `json:"write_quorum"`
	ReadQuorum       int       `json:"read_quorum"`
	FailureDomain    string    `json:"failure_domain"`
	StoragePolicy    string    `json:"storage_policy"`
	VersioningEnabled bool     `json:"versioning_enabled"`
	MaxObjectSize    int64     `json:"max_object_size"`
}

// ─── Object Metadata ──────────────────────────────────────────────────────────

// ObjectMetadata holds all metadata for an object version.
type ObjectMetadata struct {
	Bucket          string            `json:"bucket"`
	Key             string            `json:"key"`
	VersionID       string            `json:"version_id"`
	VersionSequence int64             `json:"version_sequence"`
	Size            int64             `json:"size"`
	Checksum        string            `json:"checksum"`
	ChecksumAlgo    ChecksumAlgorithm `json:"checksum_algo"`
	ContentType     string            `json:"content_type"`
	State           ObjectState       `json:"state"`
	PlacementGroup  int               `json:"placement_group"`
	ReplicaNodes    []string          `json:"replica_nodes"`
	CreatedAt       time.Time         `json:"created_at"`
	ModifiedAt      time.Time         `json:"modified_at"`
	ExpiresAt       *time.Time        `json:"expires_at,omitempty"`
	UserMetadata    map[string]string `json:"user_metadata,omitempty"`
	ETag            string            `json:"etag"`
	IsTombstone     bool              `json:"is_tombstone"`
	PreviousVersion string            `json:"previous_version,omitempty"`
	OperationID     string            `json:"operation_id,omitempty"`
	ChunkCount      int               `json:"chunk_count"`
	ChunkSize       int64             `json:"chunk_size"`
}

// ─── Replica Record ───────────────────────────────────────────────────────────

// ReplicaRecord tracks the state of one replica of an object on a storage node.
type ReplicaRecord struct {
	ID              string       `json:"id"`
	ObjectVersionID string       `json:"object_version_id"`
	Bucket          string       `json:"bucket"`
	Key             string       `json:"key"`
	VersionID       string       `json:"version_id"`
	NodeID          string       `json:"node_id"`
	State           ReplicaState `json:"state"`
	Checksum        string       `json:"checksum"`
	Size            int64        `json:"size"`
	CreatedAt       time.Time    `json:"created_at"`
	VerifiedAt      *time.Time   `json:"verified_at,omitempty"`
	RepairedAt      *time.Time   `json:"repaired_at,omitempty"`
	ScrubScheduled  *time.Time   `json:"scrub_scheduled,omitempty"`
}

// ─── Placement Group ──────────────────────────────────────────────────────────

// PlacementGroup maps a hash range to a set of storage nodes.
type PlacementGroup struct {
	ID          int      `json:"id"`
	Nodes       []string `json:"nodes"`    // ordered: [primary, secondary...]
	State       string   `json:"state"`    // ACTIVE, REBALANCING, DEGRADED
	ObjectCount int64    `json:"object_count"`
	SizeBytes   int64    `json:"size_bytes"`
}

// ─── Repair Job ───────────────────────────────────────────────────────────────

// RepairJob tracks a pending or in-progress replica repair.
type RepairJob struct {
	ID          string       `json:"id"`
	Bucket      string       `json:"bucket"`
	Key         string       `json:"key"`
	VersionID   string       `json:"version_id"`
	TargetNode  string       `json:"target_node"`
	SourceNode  string       `json:"source_node"`
	State       ReplicaState `json:"state"`
	Reason      string       `json:"reason"`
	Priority    int          `json:"priority"`
	CreatedAt   time.Time    `json:"created_at"`
	StartedAt   *time.Time   `json:"started_at,omitempty"`
	CompletedAt *time.Time   `json:"completed_at,omitempty"`
	Error       string       `json:"error,omitempty"`
	BytesCopied int64        `json:"bytes_copied"`
	TotalBytes  int64        `json:"total_bytes"`
}

// ─── Scrub Job ────────────────────────────────────────────────────────────────

// ScrubType specifies the intensity of a scrub operation.
type ScrubType string

const (
	ScrubTypeLight  ScrubType = "LIGHT"
	ScrubTypeDeep   ScrubType = "DEEP"
	ScrubTypeUrgent ScrubType = "URGENT"
)

// ScrubJob tracks a scrubbing operation on a storage node.
type ScrubJob struct {
	ID          string     `json:"id"`
	NodeID      string     `json:"node_id"`
	Type        ScrubType  `json:"type"`
	State       string     `json:"state"`
	ObjectsChecked int64   `json:"objects_checked"`
	CorruptFound   int64   `json:"corrupt_found"`
	StaleFound     int64   `json:"stale_found"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// ─── Raft Log Entry ───────────────────────────────────────────────────────────

// RaftLogEntry is a single entry in the Raft consensus log.
type RaftLogEntry struct {
	Index     int64     `json:"index"`
	Term      int64     `json:"term"`
	Operation string    `json:"operation"`
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

// ─── Cluster Membership ───────────────────────────────────────────────────────

// ClusterMembership tracks the complete membership of the storage cluster.
type ClusterMembership struct {
	Epoch        int64         `json:"epoch"`
	Nodes        []StorageNode `json:"nodes"`
	UpdatedAt    time.Time     `json:"updated_at"`
	LeaderNodeID string        `json:"leader_node_id,omitempty"`
}

// ─── Heartbeat ────────────────────────────────────────────────────────────────

// Heartbeat is the periodic status message sent by storage nodes.
type Heartbeat struct {
	NodeID         string    `json:"node_id"`
	ClusterEpoch   int64     `json:"cluster_epoch"`
	LastAppliedLog int64     `json:"last_applied_log"`
	DiskFree       int64     `json:"disk_free"`
	DiskTotal      int64     `json:"disk_total"`
	ObjectCount    int64     `json:"object_count"`
	ReplicaCount   int64     `json:"replica_count"`
	State          NodeState `json:"state"`
	Timestamp      time.Time `json:"timestamp"`
	Load           float64   `json:"load"`
}

// ─── Multipart Upload ────────────────────────────────────────────────────────

// MultipartUpload tracks an in-progress multipart upload.
type MultipartUpload struct {
	UploadID    string            `json:"upload_id"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	State       string            `json:"state"` // PENDING, COMPLETE, ABORTED
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	ExpiresAt   time.Time         `json:"expires_at"`
	PartCount   int               `json:"part_count"`
	TotalSize   int64             `json:"total_size"`
}

// MultipartPart represents one part of a multipart upload.
type MultipartPart struct {
	UploadID    string    `json:"upload_id"`
	PartNumber  int       `json:"part_number"`
	Size        int64     `json:"size"`
	Checksum    string    `json:"checksum"`
	State       string    `json:"state"` // PENDING, COMMITTED, FAILED
	NodeIDs     []string  `json:"node_ids"`
	CreatedAt   time.Time `json:"created_at"`
}

// ─── API Request/Response Types ───────────────────────────────────────────────

// PutObjectRequest is the body of a PUT object request.
type PutObjectRequest struct {
	OperationID  string            `json:"operation_id,omitempty"`
	ContentType  string            `json:"content_type"`
	Checksum     string            `json:"checksum,omitempty"`
	UserMetadata map[string]string `json:"user_metadata,omitempty"`
	IfMatch      string            `json:"if_match,omitempty"` // conditional write
}

// PutObjectResponse is the response from a successful PUT.
type PutObjectResponse struct {
	VersionID string `json:"version_id"`
	ETag      string `json:"etag"`
	Size      int64  `json:"size"`
	Checksum  string `json:"checksum"`
}

// GetObjectResponse wraps object bytes with metadata.
type GetObjectResponse struct {
	Metadata *ObjectMetadata
	Body     []byte
}

// ListObjectsRequest specifies listing parameters.
type ListObjectsRequest struct {
	Prefix            string `json:"prefix"`
	Delimiter         string `json:"delimiter"`
	Limit             int    `json:"limit"`
	ContinuationToken string `json:"continuation_token,omitempty"`
}

// ListObjectsResponse is the response to a listing request.
type ListObjectsResponse struct {
	Objects               []*ObjectMetadata `json:"objects"`
	NextContinuationToken string            `json:"next_continuation_token,omitempty"`
	IsTruncated           bool              `json:"is_truncated"`
	TotalCount            int               `json:"total_count"`
}

// ─── Errors ───────────────────────────────────────────────────────────────────

// ErrorCode is a structured error type for the distributed system.
type ErrorCode string

const (
	ErrQuorumUnavailable    ErrorCode = "QUORUM_UNAVAILABLE"
	ErrNodeUnavailable      ErrorCode = "NODE_UNAVAILABLE"
	ErrChecksumMismatch     ErrorCode = "CHECKSUM_MISMATCH"
	ErrStaleEpoch           ErrorCode = "STALE_EPOCH"
	ErrVersionConflict      ErrorCode = "VERSION_CONFLICT"
	ErrObjectNotFound       ErrorCode = "OBJECT_NOT_FOUND"
	ErrBucketNotFound       ErrorCode = "BUCKET_NOT_FOUND"
	ErrDiskFull             ErrorCode = "DISK_FULL"
	ErrRepairFailed         ErrorCode = "REPAIR_FAILED"
	ErrRebalanceFailed      ErrorCode = "REBALANCE_FAILED"
	ErrMetadataUnavailable  ErrorCode = "METADATA_UNAVAILABLE"
	ErrInvalidRequest       ErrorCode = "INVALID_REQUEST"
	ErrIdempotentRetry      ErrorCode = "IDEMPOTENT_RETRY"
	ErrReplicaCorrupt       ErrorCode = "REPLICA_CORRUPT"
	ErrUnauthorized         ErrorCode = "UNAUTHORIZED"
	ErrForbidden            ErrorCode = "FORBIDDEN"
)

// DossError is the standard structured error type for FT-DOSS.
type DossError struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	RequestID string    `json:"request_id,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
}

func (e *DossError) Error() string {
	return string(e.Code) + ": " + e.Message
}

// NewDossError creates a structured distributed system error.
func NewDossError(code ErrorCode, message string) *DossError {
	return &DossError{Code: code, Message: message}
}

// ─── Metrics Snapshot ─────────────────────────────────────────────────────────

// ClusterMetrics is a point-in-time snapshot of cluster health metrics.
type ClusterMetrics struct {
	Timestamp time.Time `json:"timestamp"`

	// Availability
	TotalNodes     int `json:"total_nodes"`
	HealthyNodes   int `json:"healthy_nodes"`
	SuspectNodes   int `json:"suspect_nodes"`
	UnavailableNodes int `json:"unavailable_nodes"`

	// Storage
	TotalObjects   int64 `json:"total_objects"`
	TotalSizeBytes int64 `json:"total_size_bytes"`
	RawCapacity    int64 `json:"raw_capacity"`
	UsableCapacity int64 `json:"usable_capacity"`
	FreeCapacity   int64 `json:"free_capacity"`

	// Durability
	DegradedObjects    int64 `json:"degraded_objects"`    // below desired replica count
	CorruptReplicas    int64 `json:"corrupt_replicas"`
	PendingRepairs     int64 `json:"pending_repairs"`
	DegradedPGs        int64 `json:"degraded_placement_groups"`
	StaleReplicas      int64 `json:"stale_replicas"`

	// Consensus
	RaftLeaderID    string `json:"raft_leader_id"`
	RaftTerm        int64  `json:"raft_term"`
	RaftCommitIndex int64  `json:"raft_commit_index"`
	ClusterEpoch    int64  `json:"cluster_epoch"`

	// Operations (rolling counters)
	TotalReads       int64   `json:"total_reads"`
	SuccessfulReads  int64   `json:"successful_reads"`
	TotalWrites      int64   `json:"total_writes"`
	SuccessfulWrites int64   `json:"successful_writes"`
	ReadLatencyP50   float64 `json:"read_latency_p50_ms"`
	ReadLatencyP99   float64 `json:"read_latency_p99_ms"`
	WriteLatencyP50  float64 `json:"write_latency_p50_ms"`
	WriteLatencyP99  float64 `json:"write_latency_p99_ms"`
}

// ─── Chunk ────────────────────────────────────────────────────────────────────

// Chunk represents a data chunk within an object stored on a node.
type Chunk struct {
	ObjectID   string `json:"object_id"`
	VersionID  string `json:"version_id"`
	ChunkIndex int    `json:"chunk_index"`
	Size       int64  `json:"size"`
	Checksum   string `json:"checksum"`
	Offset     int64  `json:"offset"`
}

// ─── Alert ────────────────────────────────────────────────────────────────────

// AlertSeverity represents how critical an alert is.
type AlertSeverity string

const (
	AlertCritical AlertSeverity = "CRITICAL"
	AlertWarning  AlertSeverity = "WARNING"
	AlertInfo     AlertSeverity = "INFO"
)

// Alert represents a system alert for the operations team.
type Alert struct {
	ID        string        `json:"id"`
	Severity  AlertSeverity `json:"severity"`
	Title     string        `json:"title"`
	Message   string        `json:"message"`
	NodeID    string        `json:"node_id,omitempty"`
	ObjectID  string        `json:"object_id,omitempty"`
	CreatedAt time.Time     `json:"created_at"`
	ResolvedAt *time.Time   `json:"resolved_at,omitempty"`
	IsActive  bool          `json:"is_active"`
}

// ─── Audit Log ────────────────────────────────────────────────────────────────

// AuditLog records security-sensitive events.
type AuditLog struct {
	ID         string    `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	Actor      string    `json:"actor"`
	Action     string    `json:"action"`
	Bucket     string    `json:"bucket,omitempty"`
	Key        string    `json:"key,omitempty"`
	VersionID  string    `json:"version_id,omitempty"`
	RequestID  string    `json:"request_id"`
	SourceIP   string    `json:"source_ip"`
	StatusCode int       `json:"status_code"`
	Details    string    `json:"details,omitempty"`
}
