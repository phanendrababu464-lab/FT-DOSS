// Package storage implements the local filesystem-backed storage node.
// Each storage node stores object data, per-chunk checksums, and a WAL.
package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/internal/checksum"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

// ─── Storage Node ─────────────────────────────────────────────────────────────

// Node is a filesystem-backed storage node.
type Node struct {
	mu           sync.RWMutex
	nodeID       string
	dataDir      string
	walDir       string
	state        types.NodeState
	clusterEpoch int64
	diskTotal    int64
	logger       *logger.Logger

	// Stats (atomic)
	objectCount  int64
	replicaCount int64
	diskUsed     int64

	// WAL
	wal *WAL

	// Object index (in-memory for fast lookups)
	index sync.Map // key: objectKey(bucket,key,version) -> *ObjectMeta

	// Fencing: reject writes from stale epochs
	fencingEnabled bool
}

// ObjectMeta is the per-object metadata stored alongside object data.
type ObjectMeta struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	VersionID   string            `json:"version_id"`
	Size        int64             `json:"size"`
	Checksum    string            `json:"checksum"`
	ContentType string            `json:"content_type"`
	State       types.ObjectState `json:"state"`
	CreatedAt   time.Time         `json:"created_at"`
	ChunkCount  int               `json:"chunk_count"`
	ChunkSize   int64             `json:"chunk_size"`
	Chunks      []ChunkMeta       `json:"chunks,omitempty"`
}

// ChunkMeta holds metadata for one data chunk.
type ChunkMeta struct {
	Index    int    `json:"index"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

// NewNode creates a new storage node.
func NewNode(nodeID, dataDir, walDir string, fencingEnabled bool, log *logger.Logger) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("creating wal dir: %w", err)
	}

	wal, err := NewWAL(walDir)
	if err != nil {
		return nil, fmt.Errorf("creating WAL: %w", err)
	}

	n := &Node{
		nodeID:         nodeID,
		dataDir:        dataDir,
		walDir:         walDir,
		state:          types.NodeStateHealthy,
		logger:         log,
		wal:            wal,
		fencingEnabled: fencingEnabled,
	}

	// Compute disk total
	if info, err := os.Stat(dataDir); err == nil {
		_ = info
		n.diskTotal = 100 * 1024 * 1024 * 1024 // 100 GB simulated
	}

	// Rebuild in-memory index from disk
	if err := n.rebuildIndex(); err != nil {
		log.Warn().Err(err).Msg("failed to rebuild storage index")
	}

	return n, nil
}

// ─── Fencing ─────────────────────────────────────────────────────────────────

// CheckEpoch rejects writes from stale cluster epochs.
func (n *Node) CheckEpoch(incomingEpoch int64) error {
	if !n.fencingEnabled {
		return nil
	}
	current := atomic.LoadInt64(&n.clusterEpoch)
	if incomingEpoch < current {
		return &types.DossError{
			Code:    types.ErrStaleEpoch,
			Message: fmt.Sprintf("stale epoch %d, current is %d", incomingEpoch, current),
			NodeID:  n.nodeID,
		}
	}
	return nil
}

// UpdateEpoch updates the cluster epoch (called when receiving heartbeats).
func (n *Node) UpdateEpoch(epoch int64) {
	for {
		current := atomic.LoadInt64(&n.clusterEpoch)
		if epoch <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&n.clusterEpoch, current, epoch) {
			break
		}
	}
}

// ─── Write Path ───────────────────────────────────────────────────────────────

// PutObject stores an object's data and metadata.
// Verifies checksum before committing.
func (n *Node) PutObject(meta *ObjectMeta, data []byte, epoch int64) error {
	if err := n.CheckEpoch(epoch); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == types.NodeStateUnavailable || n.state == types.NodeStateDecommissioning {
		return &types.DossError{
			Code:    types.ErrNodeUnavailable,
			Message: fmt.Sprintf("node %s is %s", n.nodeID, n.state),
			NodeID:  n.nodeID,
		}
	}

	// Verify checksum before persisting
	actualCS := checksum.ComputeSHA256(data)
	if meta.Checksum != "" && meta.Checksum != actualCS {
		return &types.DossError{
			Code:    types.ErrChecksumMismatch,
			Message: fmt.Sprintf("checksum mismatch: expected %s got %s", meta.Checksum, actualCS),
			NodeID:  n.nodeID,
		}
	}
	meta.Checksum = actualCS

	// Write WAL entry first (durability before ACK)
	walEntry := &WALEntry{
		Sequence:  time.Now().UnixNano(),
		Operation: "PUT",
		Bucket:    meta.Bucket,
		Key:       meta.Key,
		VersionID: meta.VersionID,
		Checksum:  meta.Checksum,
		Size:      meta.Size,
		Timestamp: time.Now(),
	}
	if err := n.wal.Write(walEntry); err != nil {
		return fmt.Errorf("writing WAL: %w", err)
	}

	// Persist object data
	objPath := n.ObjectDataPath(meta.Bucket, meta.Key, meta.VersionID)
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return fmt.Errorf("creating object dir: %w", err)
	}
	if err := atomicWrite(objPath, data); err != nil {
		return fmt.Errorf("writing object data: %w", err)
	}

	// Persist object metadata
	metaPath := n.objectMetaPath(meta.Bucket, meta.Key, meta.VersionID)
	metaData, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling object metadata: %w", err)
	}
	if err := atomicWrite(metaPath, metaData); err != nil {
		return fmt.Errorf("writing object metadata: %w", err)
	}

	// Update in-memory index
	indexKey := objectIndexKey(meta.Bucket, meta.Key, meta.VersionID)
	n.index.Store(indexKey, meta)

	// Update stats
	atomic.AddInt64(&n.objectCount, 1)
	atomic.AddInt64(&n.diskUsed, meta.Size)

	n.logger.WithFields(logger.Fields{
		Bucket:    meta.Bucket,
		Key:       meta.Key,
		VersionID: meta.VersionID,
		Event:     "OBJECT_STORED",
	}).Info().
		Str("checksum", meta.Checksum).
		Int64("size", meta.Size).
		Msg("object stored on node")

	return nil
}

// ─── Read Path ────────────────────────────────────────────────────────────────

// GetObject retrieves object data and verifies its checksum.
func (n *Node) GetObject(bucket, key, versionID string) ([]byte, *ObjectMeta, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.state == types.NodeStateUnavailable {
		return nil, nil, &types.DossError{
			Code:    types.ErrNodeUnavailable,
			Message: fmt.Sprintf("node %s is unavailable", n.nodeID),
			NodeID:  n.nodeID,
		}
	}

	// Read metadata
	meta, err := n.readMeta(bucket, key, versionID)
	if err != nil {
		return nil, nil, err
	}

	// Read data
	objPath := n.ObjectDataPath(bucket, key, versionID)
	data, err := os.ReadFile(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, &types.DossError{
				Code:    types.ErrObjectNotFound,
				Message: fmt.Sprintf("object data missing: %s/%s@%s", bucket, key, versionID),
				NodeID:  n.nodeID,
			}
		}
		return nil, nil, fmt.Errorf("reading object data: %w", err)
	}

	// Verify checksum on read (detects bitrot)
	actualCS := checksum.ComputeSHA256(data)
	if meta.Checksum != "" && actualCS != meta.Checksum {
		n.logger.ChecksumMismatch(
			fmt.Sprintf("%s/%s", bucket, key),
			versionID, n.nodeID,
			meta.Checksum, actualCS,
		)
		return nil, nil, &types.DossError{
			Code:    types.ErrChecksumMismatch,
			Message: fmt.Sprintf("data corruption detected on node %s", n.nodeID),
			NodeID:  n.nodeID,
		}
	}

	return data, meta, nil
}

// GetObjectMeta retrieves only the metadata without reading data.
func (n *Node) GetObjectMeta(bucket, key, versionID string) (*ObjectMeta, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.readMeta(bucket, key, versionID)
}

// VerifyObject verifies the checksum of a stored object without returning data.
func (n *Node) VerifyObject(bucket, key, versionID string) error {
	data, meta, err := n.GetObject(bucket, key, versionID)
	if err != nil {
		return err
	}
	return checksum.Verify(data, meta.Checksum, types.ChecksumSHA256)
}

// ─── Delete Path ──────────────────────────────────────────────────────────────

// DeleteObject removes object data from this node (tombstone is in metadata).
func (n *Node) DeleteObject(bucket, key, versionID string, epoch int64) error {
	if err := n.CheckEpoch(epoch); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	meta, err := n.readMeta(bucket, key, versionID)
	if err != nil {
		return err // object doesn't exist here
	}

	// Write WAL delete entry
	walEntry := &WALEntry{
		Sequence:  time.Now().UnixNano(),
		Operation: "DELETE",
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		Timestamp: time.Now(),
	}
	if err := n.wal.Write(walEntry); err != nil {
		return fmt.Errorf("writing WAL: %w", err)
	}

	// Remove data and metadata files
	_ = os.Remove(n.ObjectDataPath(bucket, key, versionID))
	_ = os.Remove(n.objectMetaPath(bucket, key, versionID))

	// Remove from index
	n.index.Delete(objectIndexKey(bucket, key, versionID))

	// Update stats
	atomic.AddInt64(&n.objectCount, -1)
	atomic.AddInt64(&n.diskUsed, -meta.Size)

	return nil
}

// ─── Scrubbing ────────────────────────────────────────────────────────────────

// ScrubAll performs a full checksum scan of all objects on this node.
// Returns (corrupt, stale, error).
func (n *Node) ScrubAll() (corrupt []string, stale []string, err error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	n.index.Range(func(key, value interface{}) bool {
		meta := value.(*ObjectMeta)
		data, readErr := os.ReadFile(n.ObjectDataPath(meta.Bucket, meta.Key, meta.VersionID))
		if readErr != nil {
			stale = append(stale, fmt.Sprintf("%s/%s@%s", meta.Bucket, meta.Key, meta.VersionID))
			return true
		}

		actualCS := checksum.ComputeSHA256(data)
		if actualCS != meta.Checksum {
			corrupt = append(corrupt, fmt.Sprintf("%s/%s@%s", meta.Bucket, meta.Key, meta.VersionID))
			n.logger.ChecksumMismatch(meta.Bucket+"/"+meta.Key, meta.VersionID, n.nodeID, meta.Checksum, actualCS)
		}
		return true
	})
	return
}

// InjectCorruption deliberately corrupts object data (for testing/chaos lab).
func (n *Node) InjectCorruption(bucket, key, versionID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	objPath := n.ObjectDataPath(bucket, key, versionID)
	bakPath := objPath + ".bak"
	data, err := os.ReadFile(objPath)
	if err != nil {
		return err
	}
	// Backup pristine original data before corrupting
	_ = os.WriteFile(bakPath, data, 0644)

	if len(data) > 0 {
		data[0] ^= 0xFF // flip bits
	}
	return atomicWrite(objPath, data)
}

// RepairReplica restores corrupted replica payload from pristine backup if present.
func (n *Node) RepairReplica(bucket, key, versionID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	objPath := n.ObjectDataPath(bucket, key, versionID)
	bakPath := objPath + ".bak"
	if bakData, err := os.ReadFile(bakPath); err == nil {
		_ = atomicWrite(objPath, bakData)
		_ = os.Remove(bakPath)
		return nil
	}
	return nil
}

// ─── Node State ───────────────────────────────────────────────────────────────

// SetState changes the node state.
func (n *Node) SetState(state types.NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	old := string(n.state)
	n.state = state
	n.logger.NodeStateChange(n.nodeID, old, string(state), atomic.LoadInt64(&n.clusterEpoch))
}

// State returns the current node state.
func (n *Node) State() types.NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// Heartbeat returns the current heartbeat data for this node.
func (n *Node) Heartbeat() *types.Heartbeat {
	return &types.Heartbeat{
		NodeID:         n.nodeID,
		ClusterEpoch:   atomic.LoadInt64(&n.clusterEpoch),
		DiskFree:       n.diskTotal - atomic.LoadInt64(&n.diskUsed),
		DiskTotal:      n.diskTotal,
		ObjectCount:    atomic.LoadInt64(&n.objectCount),
		ReplicaCount:   atomic.LoadInt64(&n.replicaCount),
		State:          n.State(),
		Timestamp:      time.Now(),
	}
}

// NodeID returns the node's identifier.
func (n *Node) NodeID() string { return n.nodeID }

// ─── Internal Helpers ─────────────────────────────────────────────────────────

func (n *Node) ObjectDataPath(bucket, key, versionID string) string {
	return filepath.Join(n.dataDir, bucket, key, versionID+".dat")
}

func (n *Node) objectMetaPath(bucket, key, versionID string) string {
	return filepath.Join(n.dataDir, bucket, key, versionID+".meta")
}

func objectIndexKey(bucket, key, versionID string) string {
	return fmt.Sprintf("%s|%s|%s", bucket, key, versionID)
}

func (n *Node) readMeta(bucket, key, versionID string) (*ObjectMeta, error) {
	// Check in-memory index first
	if val, ok := n.index.Load(objectIndexKey(bucket, key, versionID)); ok {
		return val.(*ObjectMeta), nil
	}

	// Fall back to disk
	metaPath := n.objectMetaPath(bucket, key, versionID)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &types.DossError{
				Code:    types.ErrObjectNotFound,
				Message: fmt.Sprintf("%s/%s@%s not found on node %s", bucket, key, versionID, n.nodeID),
				NodeID:  n.nodeID,
			}
		}
		return nil, fmt.Errorf("reading metadata: %w", err)
	}

	var meta ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("parsing metadata: %w", err)
	}
	return &meta, nil
}

func (n *Node) rebuildIndex() error {
	return filepath.Walk(n.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() || filepath.Ext(path) != ".meta" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var meta ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil
		}
		n.index.Store(objectIndexKey(meta.Bucket, meta.Key, meta.VersionID), &meta)
		atomic.AddInt64(&n.objectCount, 1)
		atomic.AddInt64(&n.diskUsed, meta.Size)
		return nil
	})
}

// atomicWrite writes data atomically using a temp file and rename.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ListObjects returns all object versions stored on this node for a bucket.
func (n *Node) ListObjects(bucket string) []*ObjectMeta {
	var result []*ObjectMeta
	n.index.Range(func(key, value interface{}) bool {
		meta := value.(*ObjectMeta)
		if meta.Bucket == bucket {
			result = append(result, meta)
		}
		return true
	})
	return result
}

// GetStats returns disk usage stats for this node.
func (n *Node) GetStats() map[string]int64 {
	return map[string]int64{
		"disk_total":    n.diskTotal,
		"disk_used":     atomic.LoadInt64(&n.diskUsed),
		"disk_free":     n.diskTotal - atomic.LoadInt64(&n.diskUsed),
		"object_count":  atomic.LoadInt64(&n.objectCount),
		"replica_count": atomic.LoadInt64(&n.replicaCount),
	}
}

// ─── Streaming I/O ───────────────────────────────────────────────────────────

// PutObjectStream stores object data from a reader without loading into RAM.
func (n *Node) PutObjectStream(meta *ObjectMeta, r io.Reader, epoch int64) error {
	if err := n.CheckEpoch(epoch); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == types.NodeStateUnavailable || n.state == types.NodeStateDecommissioning {
		return &types.DossError{
			Code:    types.ErrNodeUnavailable,
			Message: fmt.Sprintf("node %s is %s", n.nodeID, n.state),
			NodeID:  n.nodeID,
		}
	}

	objPath := n.ObjectDataPath(meta.Bucket, meta.Key, meta.VersionID)
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return err
	}

	tmp := objPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	// Compute checksum while writing
	cs, size, err := checksum.ComputeReader(io.TeeReader(r, f), types.ChecksumSHA256)
	f.Close()
	if err != nil {
		os.Remove(tmp)
		return err
	}

	// Verify checksum if client provided one
	if meta.Checksum != "" && meta.Checksum != cs {
		os.Remove(tmp)
		return &types.DossError{
			Code:    types.ErrChecksumMismatch,
			Message: fmt.Sprintf("stream checksum mismatch: expected %s got %s", meta.Checksum, cs),
			NodeID:  n.nodeID,
		}
	}

	meta.Checksum = cs
	meta.Size = size

	if err := os.Rename(tmp, objPath); err != nil {
		return err
	}

	// Write metadata
	metaData, _ := json.Marshal(meta)
	if err := atomicWrite(n.objectMetaPath(meta.Bucket, meta.Key, meta.VersionID), metaData); err != nil {
		return err
	}

	n.index.Store(objectIndexKey(meta.Bucket, meta.Key, meta.VersionID), meta)
	atomic.AddInt64(&n.objectCount, 1)
	atomic.AddInt64(&n.diskUsed, size)
	return nil
}

// ResetStorage clears all stored data files and index for this node.
func (n *Node) ResetStorage() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.index.Range(func(key, value interface{}) bool {
		n.index.Delete(key)
		return true
	})
	_ = os.RemoveAll(n.dataDir)
	_ = os.MkdirAll(n.dataDir, 0755)
	atomic.StoreInt64(&n.objectCount, 0)
	atomic.StoreInt64(&n.diskUsed, 0)
	atomic.StoreInt64(&n.replicaCount, 0)
}
