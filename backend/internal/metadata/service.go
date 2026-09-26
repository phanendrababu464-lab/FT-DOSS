// Package metadata implements the consensus-backed metadata service using Raft.
// Object versions, tombstones, conditional writes, and cluster state are all
// mediated through this service. No object becomes visible without a committed
// metadata entry.
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/internal/raft"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
	"github.com/google/uuid"
)

// ─── State Machine ────────────────────────────────────────────────────────────

// Service is the metadata service with Raft-backed consistency.
type Service struct {
	mu       sync.RWMutex
	raftNode *raft.Node
	db       *sql.DB // PostgreSQL for persistence
	logger   *logger.Logger
	nodeID   string

	// In-memory state (built from Raft log, fast read path)
	objects     map[string]*types.ObjectMetadata    // bucket/key -> current version
	versions    map[string]*types.ObjectMetadata    // bucket/key/version -> metadata
	buckets     map[string]*types.Bucket
	tombstones  map[string]string                   // bucket/key -> tombstone versionID
	operations  map[string]*types.PutObjectResponse // operationID -> result (idempotency)

	// Sequence counter (monotonically increasing, assigned by leader)
	sequence int64
}

// ─── Constructor ──────────────────────────────────────────────────────────────

// NewService creates the metadata service.
func NewService(nodeID string, raftNode *raft.Node, db *sql.DB, log *logger.Logger) *Service {
	s := &Service{
		raftNode:   raftNode,
		db:         db,
		logger:     log,
		nodeID:     nodeID,
		objects:    make(map[string]*types.ObjectMetadata),
		versions:   make(map[string]*types.ObjectMetadata),
		buckets:    make(map[string]*types.Bucket),
		tombstones: make(map[string]string),
		operations: make(map[string]*types.PutObjectResponse),
	}
	return s
}

// SetRaftNode sets the Raft node after creation (for forward-reference wiring).
func (s *Service) SetRaftNode(raftNode *raft.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raftNode = raftNode
}

// ─── Raft State Machine Interface ────────────────────────────────────────────

// Apply processes a committed Raft log entry.
// This is the ONLY place state is mutated — ensuring consistency.
func (s *Service) Apply(cmd raft.LogCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Type {
	case raft.CmdPutObject:
		return s.applyPutObject(cmd)
	case raft.CmdDeleteObject:
		return s.applyDeleteObject(cmd)
	case raft.CmdPutBucket:
		return s.applyPutBucket(cmd)
	case raft.CmdClusterEpoch:
		return s.applyClusterEpoch(cmd)
	case raft.CmdNoop:
		return nil
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// Snapshot creates a point-in-time snapshot of the metadata state.
func (s *Service) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state := struct {
		Objects    map[string]*types.ObjectMetadata    `json:"objects"`
		Versions   map[string]*types.ObjectMetadata    `json:"versions"`
		Buckets    map[string]*types.Bucket            `json:"buckets"`
		Tombstones map[string]string                   `json:"tombstones"`
		Operations map[string]*types.PutObjectResponse `json:"operations"`
		Sequence   int64                               `json:"sequence"`
	}{
		Objects:    s.objects,
		Versions:   s.versions,
		Buckets:    s.buckets,
		Tombstones: s.tombstones,
		Operations: s.operations,
		Sequence:   atomic.LoadInt64(&s.sequence),
	}
	return json.Marshal(state)
}

// Restore rebuilds state from a snapshot.
func (s *Service) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := struct {
		Objects    map[string]*types.ObjectMetadata    `json:"objects"`
		Versions   map[string]*types.ObjectMetadata    `json:"versions"`
		Buckets    map[string]*types.Bucket            `json:"buckets"`
		Tombstones map[string]string                   `json:"tombstones"`
		Operations map[string]*types.PutObjectResponse `json:"operations"`
		Sequence   int64                               `json:"sequence"`
	}{}

	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	s.objects = state.Objects
	s.versions = state.Versions
	s.buckets = state.Buckets
	s.tombstones = state.Tombstones
	s.operations = state.Operations
	atomic.StoreInt64(&s.sequence, state.Sequence)
	return nil
}

// ─── Bucket Operations ────────────────────────────────────────────────────────

// CreateBucket creates a new bucket via Raft consensus.
func (s *Service) CreateBucket(ctx context.Context, bucket *types.Bucket) error {
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("not leader: redirect to %s", s.raftNode.LeaderID())
	}

	payload, _ := json.Marshal(bucket)
	cmd := raft.LogCommand{
		Type:    raft.CmdPutBucket,
		Payload: payload,
	}

	idx, _, err := s.raftNode.Propose(ctx, cmd)
	if err != nil {
		return err
	}
	return s.raftNode.WaitForCommit(ctx, idx)
}

// GetBucket returns bucket configuration.
func (s *Service) GetBucket(name string) (*types.Bucket, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.buckets[name]
	if !ok {
		return nil, &types.DossError{
			Code:    types.ErrBucketNotFound,
			Message: fmt.Sprintf("bucket %s not found", name),
		}
	}
	result := *b
	return &result, nil
}

// ListBuckets returns all buckets.
func (s *Service) ListBuckets() []*types.Bucket {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.Bucket
	for _, b := range s.buckets {
		bCopy := *b
		result = append(result, &bCopy)
	}
	return result
}

// ─── Object Write ────────────────────────────────────────────────────────────

// PutObjectMeta atomically commits new object metadata via Raft.
// Supports conditional writes (IF-MATCH) and idempotency tokens.
// INVARIANT: An object becomes VISIBLE only after this commit.
func (s *Service) PutObjectMeta(ctx context.Context, req *PutMetaRequest) (*types.ObjectMetadata, error) {
	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader: redirect to %s", s.raftNode.LeaderID())
	}

	// Check idempotency token
	if req.OperationID != "" {
		s.mu.RLock()
		if existing, ok := s.operations[req.OperationID]; ok {
			s.mu.RUnlock()
			// Return the original result
			existingMeta := s.getObjectMetaLocked(req.Bucket, req.Key)
			if existingMeta != nil {
				return existingMeta, nil
			}
			_ = existing
		}
		s.mu.RUnlock()
	}

	// Check conditional write (IF-MATCH)
	if req.IfMatch != "" {
		s.mu.RLock()
		current := s.getObjectMetaLocked(req.Bucket, req.Key)
		s.mu.RUnlock()

		if current == nil && req.IfMatch != "*" {
			// Object doesn't exist but client expects it to
			return nil, &types.DossError{
				Code:    types.ErrVersionConflict,
				Message: fmt.Sprintf("IF-MATCH %s: object not found", req.IfMatch),
			}
		}
		if current != nil && req.IfMatch != current.VersionID && req.IfMatch != "*" {
			return nil, &types.DossError{
				Code:    types.ErrVersionConflict,
				Message: fmt.Sprintf("IF-MATCH failed: expected %s, current is %s", req.IfMatch, current.VersionID),
			}
		}
	}

	// Assign new version and sequence
	seq := atomic.AddInt64(&s.sequence, 1)
	versionID := req.VersionID
	if versionID == "" {
		versionID = fmt.Sprintf("v%d", seq)
	}
	meta := &types.ObjectMetadata{
		Bucket:          req.Bucket,
		Key:             req.Key,
		VersionID:       versionID,
		VersionSequence: seq,
		Size:            req.Size,
		Checksum:        req.Checksum,
		ChecksumAlgo:    types.ChecksumSHA256,
		ContentType:     req.ContentType,
		State:           types.ObjectStateVisible,
		PlacementGroup:  req.PlacementGroup,
		ReplicaNodes:    req.ReplicaNodes,
		CreatedAt:       time.Now(),
		ModifiedAt:      time.Now(),
		ETag:            req.Checksum,
		OperationID:     req.OperationID,
	}

	// Set previous version
	s.mu.RLock()
	current := s.getObjectMetaLocked(req.Bucket, req.Key)
	s.mu.RUnlock()
	if current != nil {
		meta.PreviousVersion = current.VersionID
	}

	payload, _ := json.Marshal(meta)
	cmd := raft.LogCommand{
		Type:    raft.CmdPutObject,
		Payload: payload,
	}

	idx, _, err := s.raftNode.Propose(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("Raft propose failed: %w", err)
	}

	if err := s.raftNode.WaitForCommit(ctx, idx); err != nil {
		return nil, fmt.Errorf("Raft commit timeout: %w", err)
	}

	// Read back committed state
	s.mu.RLock()
	defer s.mu.RUnlock()
	committed := s.getObjectMetaLocked(req.Bucket, req.Key)
	if committed == nil {
		return nil, fmt.Errorf("metadata not found after commit")
	}
	result := *committed
	return &result, nil
}

// ─── Object Read ─────────────────────────────────────────────────────────────

// GetObjectMeta returns the current visible version of an object.
// INVARIANT: Only VISIBLE state objects are returned.
func (s *Service) GetObjectMeta(bucket, key string) (*types.ObjectMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check for tombstone first
	tombKey := objectKey(bucket, key)
	if _, isTombstoned := s.tombstones[tombKey]; isTombstoned {
		return nil, &types.DossError{
			Code:    types.ErrObjectNotFound,
			Message: fmt.Sprintf("%s/%s has been deleted (tombstone exists)", bucket, key),
		}
	}

	meta := s.getObjectMetaLocked(bucket, key)
	if meta == nil {
		return nil, &types.DossError{
			Code:    types.ErrObjectNotFound,
			Message: fmt.Sprintf("%s/%s not found", bucket, key),
		}
	}
	result := *meta
	return &result, nil
}

// GetObjectVersion returns a specific version of an object.
func (s *Service) GetObjectVersion(bucket, key, versionID string) (*types.ObjectMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vKey := versionKey(bucket, key, versionID)
	meta, ok := s.versions[vKey]
	if !ok {
		return nil, &types.DossError{
			Code:    types.ErrObjectNotFound,
			Message: fmt.Sprintf("%s/%s@%s not found", bucket, key, versionID),
		}
	}
	result := *meta
	return &result, nil
}

// ─── Object Delete ────────────────────────────────────────────────────────────

// DeleteObject creates a tombstone for an object via Raft.
// INVARIANT: Tombstones cannot be overwritten by older versions.
func (s *Service) DeleteObject(ctx context.Context, bucket, key string) error {
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("not leader")
	}

	// Check object exists
	s.mu.RLock()
	current := s.getObjectMetaLocked(bucket, key)
	s.mu.RUnlock()

	if current == nil {
		return &types.DossError{
			Code:    types.ErrObjectNotFound,
			Message: fmt.Sprintf("%s/%s not found", bucket, key),
		}
	}

	// Create tombstone version
	seq := atomic.AddInt64(&s.sequence, 1)
	tombstone := &types.ObjectMetadata{
		Bucket:          bucket,
		Key:             key,
		VersionID:       fmt.Sprintf("v%d", seq),
		VersionSequence: seq,
		State:           types.ObjectStateTombstone,
		IsTombstone:     true,
		PreviousVersion: current.VersionID,
		CreatedAt:       time.Now(),
		ModifiedAt:      time.Now(),
	}

	payload, _ := json.Marshal(tombstone)
	cmd := raft.LogCommand{
		Type:    raft.CmdDeleteObject,
		Payload: payload,
	}

	idx, _, err := s.raftNode.Propose(ctx, cmd)
	if err != nil {
		return err
	}
	return s.raftNode.WaitForCommit(ctx, idx)
}

// ─── List ─────────────────────────────────────────────────────────────────────

// ListObjects returns objects in a bucket matching the given criteria.
func (s *Service) ListObjects(bucket, prefix, delimiter, continuationToken string, limit int) (*types.ListObjectsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var objects []*types.ObjectMetadata
	for k, meta := range s.objects {
		if meta.Bucket != bucket {
			continue
		}
		if _, isTombstoned := s.tombstones[k]; isTombstoned {
			continue
		}
		if prefix != "" && len(meta.Key) < len(prefix) {
			continue
		}
		if prefix != "" && meta.Key[:len(prefix)] != prefix {
			continue
		}
		if meta.State != types.ObjectStateVisible {
			continue
		}
		obj := *meta
		objects = append(objects, &obj)
	}

	// Apply continuation token
	start := 0
	if continuationToken != "" {
		for i, obj := range objects {
			if obj.Key > continuationToken {
				start = i
				break
			}
		}
	}

	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	end := start + limit
	isTruncated := false
	if end >= len(objects) {
		end = len(objects)
	} else {
		isTruncated = true
	}

	var nextToken string
	if isTruncated && end > 0 {
		nextToken = objects[end-1].Key
	}

	return &types.ListObjectsResponse{
		Objects:               objects[start:end],
		NextContinuationToken: nextToken,
		IsTruncated:           isTruncated,
		TotalCount:            len(objects),
	}, nil
}

// ─── Internal Apply Handlers ─────────────────────────────────────────────────

func (s *Service) applyPutObject(cmd raft.LogCommand) error {
	var meta types.ObjectMetadata
	if err := json.Unmarshal(cmd.Payload, &meta); err != nil {
		return err
	}

	key := objectKey(meta.Bucket, meta.Key)
	vKey := versionKey(meta.Bucket, meta.Key, meta.VersionID)

	s.objects[key] = &meta
	s.versions[vKey] = &meta

	// Store idempotency result
	if meta.OperationID != "" {
		s.operations[meta.OperationID] = &types.PutObjectResponse{
			VersionID: meta.VersionID,
			ETag:      meta.ETag,
			Size:      meta.Size,
			Checksum:  meta.Checksum,
		}
	}

	s.logger.WithFields(logger.Fields{
		Bucket:    meta.Bucket,
		Key:       meta.Key,
		VersionID: meta.VersionID,
		Event:     "METADATA_COMMITTED",
	}).Info().
		Str("checksum", meta.Checksum).
		Int64("size", meta.Size).
		Msg("object metadata committed via Raft")

	return nil
}

func (s *Service) applyDeleteObject(cmd raft.LogCommand) error {
	var tombstone types.ObjectMetadata
	if err := json.Unmarshal(cmd.Payload, &tombstone); err != nil {
		return err
	}

	key := objectKey(tombstone.Bucket, tombstone.Key)
	vKey := versionKey(tombstone.Bucket, tombstone.Key, tombstone.VersionID)

	// Record tombstone - prevents resurrection of stale objects
	s.tombstones[key] = tombstone.VersionID
	s.versions[vKey] = &tombstone
	delete(s.objects, key)

	s.logger.WithFields(logger.Fields{
		Bucket:    tombstone.Bucket,
		Key:       tombstone.Key,
		VersionID: tombstone.VersionID,
		Event:     "TOMBSTONE_CREATED",
	}).Info().Msg("object tombstoned via Raft")

	return nil
}

func (s *Service) applyPutBucket(cmd raft.LogCommand) error {
	var bucket types.Bucket
	if err := json.Unmarshal(cmd.Payload, &bucket); err != nil {
		return err
	}
	s.buckets[bucket.Name] = &bucket
	return nil
}

func (s *Service) applyClusterEpoch(cmd raft.LogCommand) error {
	var epoch struct{ Epoch int64 }
	if err := json.Unmarshal(cmd.Payload, &epoch); err != nil {
		return err
	}
	atomic.StoreInt64(&s.sequence, epoch.Epoch)
	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (s *Service) getObjectMetaLocked(bucket, key string) *types.ObjectMetadata {
	return s.objects[objectKey(bucket, key)]
}

func objectKey(bucket, key string) string {
	return bucket + "/" + key
}

func versionKey(bucket, key, version string) string {
	return bucket + "/" + key + "@" + version
}

// IsLeader returns whether this node is the Raft leader.
func (s *Service) IsLeader() bool {
	return s.raftNode.IsLeader()
}

// LeaderID returns the current Raft leader ID.
func (s *Service) LeaderID() string {
	return s.raftNode.LeaderID()
}

// GetRaftStatus returns the Raft status for the dashboard.
func (s *Service) GetRaftStatus() map[string]interface{} {
	return s.raftNode.GetStatus()
}

// GetAllVersions returns all versions of an object (for version history).
func (s *Service) GetAllVersions(bucket, key string) []*types.ObjectMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := bucket + "/" + key + "@"
	var result []*types.ObjectMetadata
	for k, v := range s.versions {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			obj := *v
			result = append(result, &obj)
		}
	}
	return result
}

// GenerateVersionID creates a new unique version ID.
func GenerateVersionID() string {
	return uuid.New().String()
}

// ResetState resets in-memory metadata maps to pristine baseline state.
func (s *Service) ResetState() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects = make(map[string]*types.ObjectMetadata)
	s.versions = make(map[string]*types.ObjectMetadata)
	s.tombstones = make(map[string]string)
	s.operations = make(map[string]*types.PutObjectResponse)
	s.sequence = 0

	s.buckets = make(map[string]*types.Bucket)
	s.buckets["default"] = &types.Bucket{
		Name:              "default",
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		VersioningEnabled: true,
		CreatedAt:         time.Now(),
	}
}

// ─── Request Types ────────────────────────────────────────────────────────────

// PutMetaRequest is the request to commit object metadata.
type PutMetaRequest struct {
	Bucket         string
	Key            string
	VersionID      string // pre-assigned version ID (must match storage)
	Size           int64
	Checksum       string
	ContentType    string
	PlacementGroup int
	ReplicaNodes   []string
	OperationID    string
	IfMatch        string // conditional write
}
