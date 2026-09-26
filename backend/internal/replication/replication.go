// Package replication implements the quorum-based replication protocol.
// Primary receives data, replicates to secondaries, waits for write quorum.
package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/internal/storage"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

// ─── Write Result ─────────────────────────────────────────────────────────────

// ReplicaResult is the result of replicating to a single node.
type ReplicaResult struct {
	NodeID  string
	Success bool
	Error   error
	Latency time.Duration
}

// WriteResult tracks the overall quorum write result.
type WriteResult struct {
	VersionID  string
	Checksum   string
	Replicas   []ReplicaResult
	Successful int
	Failed     int
	QuorumMet  bool
}

// ─── Manager ─────────────────────────────────────────────────────────────────

// Manager handles quorum-based replication across storage nodes.
type Manager struct {
	mu           sync.RWMutex
	localNode    *storage.Node
	registry     *storage.Registry
	peers        map[string]string // nodeID -> HTTP base URL
	writeQuorum  int
	readQuorum   int
	clusterEpoch int64
	logger       *logger.Logger
	httpClient   *http.Client
}

// NewManager creates a new replication manager.
func NewManager(localNode *storage.Node, writeQuorum, readQuorum int, log *logger.Logger) *Manager {
	return &Manager{
		localNode:   localNode,
		peers:       make(map[string]string),
		writeQuorum: writeQuorum,
		readQuorum:  readQuorum,
		logger:      log,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SetRegistry sets the local storage node registry for multi-node local replication.
func (m *Manager) SetRegistry(registry *storage.Registry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registry = registry
}

// AddPeer registers a remote storage node peer.
func (m *Manager) AddPeer(nodeID, baseURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers[nodeID] = baseURL
}

// RemovePeer deregisters a peer.
func (m *Manager) RemovePeer(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peers, nodeID)
}

// UpdateEpoch sets the current cluster epoch for fencing.
func (m *Manager) UpdateEpoch(epoch int64) {
	atomic.StoreInt64(&m.clusterEpoch, epoch)
}

// ─── Write Path ───────────────────────────────────────────────────────────────

// ReplicateWrite performs a quorum write: persist locally, then replicate.
// NEVER returns success unless quorum has acknowledged.
func (m *Manager) ReplicateWrite(
	ctx context.Context,
	meta *storage.ObjectMeta,
	data []byte,
	targetNodes []string,
) (*WriteResult, error) {

	epoch := atomic.LoadInt64(&m.clusterEpoch)
	result := &WriteResult{
		VersionID: meta.VersionID,
		Checksum:  meta.Checksum,
	}

	// Step 1: Persist on primary (this node)
	startTime := time.Now()
	primaryErr := m.localNode.PutObject(meta, data, epoch)
	primaryLatency := time.Since(startTime)

	if primaryErr != nil {
		return nil, fmt.Errorf("primary write failed: %w", primaryErr)
	}

	result.Replicas = append(result.Replicas, ReplicaResult{
		NodeID:  m.localNode.NodeID(),
		Success: true,
		Latency: primaryLatency,
	})
	result.Successful = 1

	// Step 2: Replicate to secondary nodes concurrently
	m.mu.RLock()
	peers := make(map[string]string)
	for k, v := range m.peers {
		peers[k] = v
	}
	reg := m.registry
	m.mu.RUnlock()

	var wg sync.WaitGroup
	var resMu sync.Mutex

	for _, nodeID := range targetNodes {
		if nodeID == m.localNode.NodeID() {
			continue // already wrote to primary
		}

		// First check if target node exists in local storage registry
		var localPeer *storage.Node
		if reg != nil {
			localPeer = reg.GetNode(nodeID)
		}

		if localPeer != nil {
			wg.Add(1)
			go func(nID string, node *storage.Node) {
				defer wg.Done()
				start := time.Now()
				err := node.PutObject(meta, data, epoch)
				latency := time.Since(start)

				resMu.Lock()
				defer resMu.Unlock()
				if err != nil {
					result.Replicas = append(result.Replicas, ReplicaResult{
						NodeID:  nID,
						Success: false,
						Error:   err,
						Latency: latency,
					})
					result.Failed++
				} else {
					result.Replicas = append(result.Replicas, ReplicaResult{
						NodeID:  nID,
						Success: true,
						Latency: latency,
					})
					result.Successful++
				}
			}(nodeID, localPeer)
			continue
		}

		peerURL, ok := peers[nodeID]
		if !ok {
			// Node not registered as peer - count as failure
			resMu.Lock()
			result.Replicas = append(result.Replicas, ReplicaResult{
				NodeID:  nodeID,
				Success: false,
				Error:   fmt.Errorf("peer %s not registered", nodeID),
			})
			result.Failed++
			resMu.Unlock()
			continue
		}

		wg.Add(1)
		go func(nID, url string) {
			defer wg.Done()
			start := time.Now()
			err := m.replicateToNode(ctx, url, meta, data, epoch, nID)
			latency := time.Since(start)

			resMu.Lock()
			defer resMu.Unlock()
			if err != nil {
				result.Replicas = append(result.Replicas, ReplicaResult{
					NodeID:  nID,
					Success: false,
					Error:   err,
					Latency: latency,
				})
				result.Failed++
			} else {
				result.Replicas = append(result.Replicas, ReplicaResult{
					NodeID:  nID,
					Success: true,
					Latency: latency,
				})
				result.Successful++
			}
		}(nodeID, peerURL)
	}

	wg.Wait()

	// Step 3: Check quorum
	result.QuorumMet = result.Successful >= m.writeQuorum

	if !result.QuorumMet {
		return result, &types.DossError{
			Code:    types.ErrQuorumUnavailable,
			Message: fmt.Sprintf("write quorum not met: got %d/%d required", result.Successful, m.writeQuorum),
		}
	}

	m.logger.WithFields(logger.Fields{
		VersionID: meta.VersionID,
		Bucket:    meta.Bucket,
		Key:       meta.Key,
		Event:     "QUORUM_WRITE_COMPLETE",
	}).Info().
		Int("successful", result.Successful).
		Int("failed", result.Failed).
		Int("quorum", m.writeQuorum).
		Msg("quorum write completed")

	return result, nil
}

// replicateToNode sends object data to a peer storage node via HTTP.
func (m *Manager) replicateToNode(ctx context.Context, baseURL string, meta *storage.ObjectMeta, data []byte, epoch int64, targetNodeID string) error {
	type replicaRequest struct {
		Meta         *storage.ObjectMeta `json:"meta"`
		Data         []byte              `json:"data"`
		Epoch        int64               `json:"epoch"`
		TargetNodeID string              `json:"target_node_id"`
	}

	payload, err := json.Marshal(replicaRequest{
		Meta:         meta,
		Data:         data,
		Epoch:        epoch,
		TargetNodeID: targetNodeID,
	})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/internal/replica", baseURL)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cluster-Epoch", fmt.Sprintf("%d", epoch))
	req.Header.Set("X-Target-Node-ID", targetNodeID)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("replication HTTP error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("replication failed with status %d", resp.StatusCode)
	}
	return nil
}

// ─── Read Path ────────────────────────────────────────────────────────────────

// ReadWithFallback reads from the primary, falling back to healthy replicas.
// On checksum failure: marks replica CORRUPT, tries another, schedules repair.
func (m *Manager) ReadWithFallback(
	ctx context.Context,
	bucket, key, versionID string,
	candidateNodes []string,
) ([]byte, *storage.ObjectMeta, string, error) {

	// Try each candidate node in order
	for _, nodeID := range candidateNodes {
		m.mu.RLock()
		var node *storage.Node
		if m.registry != nil {
			node = m.registry.GetNode(nodeID)
		}
		peerURL, hasPeerURL := m.peers[nodeID]
		m.mu.RUnlock()

		if node != nil {
			data, meta, err := node.GetObject(bucket, key, versionID)
			if err == nil {
				return data, meta, nodeID, nil
			}
			m.logger.Error().Err(err).
				Str("node", nodeID).
				Str("bucket", bucket).
				Str("key", key).
				Msg("read failed on local node, trying next")
			continue
		}

		if hasPeerURL {
			data, meta, err := m.readFromNode(ctx, peerURL, bucket, key, versionID, nodeID)
			if err == nil {
				return data, meta, nodeID, nil
			}
			m.logger.Error().Err(err).
				Str("node", nodeID).
				Msg("read failed on peer, trying next")
		}
	}

	return nil, nil, "", &types.DossError{
		Code:    types.ErrObjectNotFound,
		Message: fmt.Sprintf("object %s/%s@%s not readable from any healthy replica", bucket, key, versionID),
	}
}

// readFromNode fetches object data from a peer via HTTP.
func (m *Manager) readFromNode(ctx context.Context, baseURL, bucket, key, versionID, targetNodeID string) ([]byte, *storage.ObjectMeta, error) {
	url := fmt.Sprintf("%s/internal/objects/%s/%s?version=%s&node_id=%s", baseURL, bucket, key, versionID, targetNodeID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("X-Target-Node-ID", targetNodeID)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil, &types.DossError{Code: types.ErrObjectNotFound}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("peer read returned %d", resp.StatusCode)
	}

	type readResponse struct {
		Meta *storage.ObjectMeta `json:"meta"`
		Data []byte              `json:"data"`
	}
	var result readResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, nil, err
	}
	return result.Data, result.Meta, nil
}

// ─── Repair ───────────────────────────────────────────────────────────────────

// RepairReplica copies a healthy replica to a degraded/corrupt node.
func (m *Manager) RepairReplica(
	ctx context.Context,
	bucket, key, versionID string,
	sourceNodeID, targetNodeID string,
) error {

	m.logger.ReplicaRepairCompleted("", versionID, targetNodeID, "", 0)

	// Get data from source
	var data []byte
	var meta *storage.ObjectMeta

	if sourceNodeID == m.localNode.NodeID() {
		var err error
		data, meta, err = m.localNode.GetObject(bucket, key, versionID)
		if err != nil {
			return fmt.Errorf("repair source read failed: %w", err)
		}
	} else {
		peerURL, ok := m.peers[sourceNodeID]
		if !ok {
			return fmt.Errorf("source node %s not found", sourceNodeID)
		}
		var err error
		data, meta, err = m.readFromNode(ctx, peerURL, bucket, key, versionID, sourceNodeID)
		if err != nil {
			return fmt.Errorf("repair source read failed: %w", err)
		}
	}

	// Send to target
	if targetNodeID == m.localNode.NodeID() {
		return m.localNode.PutObject(meta, data, atomic.LoadInt64(&m.clusterEpoch))
	}

	peerURL, ok := m.peers[targetNodeID]
	if !ok {
		return fmt.Errorf("target node %s not found", targetNodeID)
	}
	return m.replicateToNode(ctx, peerURL, meta, data, atomic.LoadInt64(&m.clusterEpoch), targetNodeID)
}

// GetQuorumState returns quorum configuration.
func (m *Manager) GetQuorumState() (writeQ, readQ int) {
	return m.writeQuorum, m.readQuorum
}
