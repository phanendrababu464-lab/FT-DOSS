// Package membership implements the node health/failure detection system.
// Uses heartbeats, generation numbers, and cluster epochs.
package membership

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

// ─── Tracker ─────────────────────────────────────────────────────────────────

// Tracker monitors storage node health via heartbeats.
type Tracker struct {
	mu              sync.RWMutex
	nodes           map[string]*NodeRecord
	clusterEpoch    int64
	suspectTimeout  time.Duration
	unavailTimeout  time.Duration
	logger          *logger.Logger
	stopCh          chan struct{}
	onStateChange   func(nodeID string, oldState, newState types.NodeState)

	// Event channel for dashboard
	eventCh chan MembershipEvent
}

// NodeRecord tracks the state of a single node.
type NodeRecord struct {
	Node          *types.StorageNode
	LastHeartbeat time.Time
	GenerationNum int64
	SuspectSince  *time.Time
}

// MembershipEvent is emitted when node state changes.
type MembershipEvent struct {
	Type      string
	NodeID    string
	OldState  types.NodeState
	NewState  types.NodeState
	Timestamp time.Time
	Epoch     int64
}

// NewTracker creates a new membership tracker.
func NewTracker(
	suspectTimeout, unavailTimeout time.Duration,
	log *logger.Logger,
) *Tracker {
	return &Tracker{
		nodes:          make(map[string]*NodeRecord),
		suspectTimeout: suspectTimeout,
		unavailTimeout: unavailTimeout,
		logger:         log,
		stopCh:         make(chan struct{}),
		eventCh:        make(chan MembershipEvent, 256),
	}
}

// SetStateChangeCallback sets a callback for node state changes.
func (t *Tracker) SetStateChangeCallback(fn func(nodeID string, oldState, newState types.NodeState)) {
	t.onStateChange = fn
}

// Events returns the membership event channel.
func (t *Tracker) Events() <-chan MembershipEvent { return t.eventCh }

// Start begins the background health checking loop.
func (t *Tracker) Start() {
	go t.healthCheckLoop()
}

// Stop halts the health checker.
func (t *Tracker) Stop() {
	close(t.stopCh)
}

// RegisterNode adds a node to the tracker.
func (t *Tracker) RegisterNode(node *types.StorageNode) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.nodes[node.ID]; !exists {
		t.nodes[node.ID] = &NodeRecord{
			Node:          node,
			LastHeartbeat: time.Now(),
			GenerationNum: 1,
		}
		t.logger.NodeStateChange(node.ID, "", string(node.State), atomic.LoadInt64(&t.clusterEpoch))
		t.emitEvent("NODE_JOINED", node.ID, "", node.State)
	}
}

// DeregisterNode removes a node from the tracker.
func (t *Tracker) DeregisterNode(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if rec, ok := t.nodes[nodeID]; ok {
		t.emitEvent("NODE_LEFT", nodeID, rec.Node.State, types.NodeStateUnavailable)
		delete(t.nodes, nodeID)
	}
}

// ProcessHeartbeat handles an incoming heartbeat from a storage node.
func (t *Tracker) ProcessHeartbeat(hb *types.Heartbeat) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	currentEpoch := atomic.LoadInt64(&t.clusterEpoch)

	rec, ok := t.nodes[hb.NodeID]
	if !ok {
		return fmt.Errorf("unknown node: %s", hb.NodeID)
	}

	// Fencing: reject heartbeats from nodes with stale epochs
	if hb.ClusterEpoch < currentEpoch {
		t.logger.Warn().
			Str("node_id", hb.NodeID).
			Int64("hb_epoch", hb.ClusterEpoch).
			Int64("current_epoch", currentEpoch).
			Msg("rejected heartbeat from stale epoch")
		return fmt.Errorf("stale epoch %d (current: %d)", hb.ClusterEpoch, currentEpoch)
	}

	// Update node record
	rec.LastHeartbeat = time.Now()
	rec.Node.DiskFree = hb.DiskFree
	rec.Node.DiskTotal = hb.DiskTotal
	rec.Node.ObjectCount = hb.ObjectCount
	rec.Node.ReplicaCount = hb.ReplicaCount
	rec.Node.LastHeartbeat = rec.LastHeartbeat
	rec.Node.ClusterEpoch = hb.ClusterEpoch

	// Recover suspect nodes when heartbeat arrives
	oldState := rec.Node.State
	if oldState == types.NodeStateSuspect {
		rec.Node.State = types.NodeStateHealthy
		rec.SuspectSince = nil
		t.changeState(rec, oldState, types.NodeStateHealthy)
	}

	return nil
}

// AddNode registers a new node and increments the cluster epoch.
func (t *Tracker) AddNode(node *types.StorageNode) int64 {
	t.RegisterNode(node)
	return t.IncrementEpoch()
}

// RemoveNode marks a node for decommissioning, removes it from active membership, and increments epoch.
func (t *Tracker) RemoveNode(nodeID string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if rec, ok := t.nodes[nodeID]; ok {
		oldState := rec.Node.State
		rec.Node.State = types.NodeStateDecommissioning
		t.changeState(rec, oldState, types.NodeStateDecommissioning)
		t.emitEvent("NODE_REMOVED", nodeID, oldState, types.NodeStateDecommissioning)
		delete(t.nodes, nodeID)
	}

	return t.incrementEpochLocked()
}

// IncrementEpoch increases the cluster epoch (called on membership changes).
func (t *Tracker) IncrementEpoch() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.incrementEpochLocked()
}

func (t *Tracker) incrementEpochLocked() int64 {
	newEpoch := atomic.AddInt64(&t.clusterEpoch, 1)
	t.logger.Info().
		Int64("epoch", newEpoch).
		Msg("cluster epoch incremented")
	return newEpoch
}

// GetEpoch returns the current cluster epoch.
func (t *Tracker) GetEpoch() int64 {
	return atomic.LoadInt64(&t.clusterEpoch)
}

// GetNode returns a copy of the node record.
func (t *Tracker) GetNode(nodeID string) (*types.StorageNode, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.nodes[nodeID]
	if !ok {
		return nil, false
	}
	node := *rec.Node
	return &node, true
}

// GetAllNodes returns all tracked nodes.
func (t *Tracker) GetAllNodes() []*types.StorageNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []*types.StorageNode
	for _, rec := range t.nodes {
		node := *rec.Node
		result = append(result, &node)
	}
	return result
}

// GetHealthyNodes returns only healthy nodes.
func (t *Tracker) GetHealthyNodes() []*types.StorageNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []*types.StorageNode
	for _, rec := range t.nodes {
		if rec.Node.State == types.NodeStateHealthy {
			node := *rec.Node
			result = append(result, &node)
		}
	}
	return result
}

// SetNodeState directly sets a node's state (for chaos lab / admin operations).
func (t *Tracker) SetNodeState(nodeID string, state types.NodeState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	oldState := rec.Node.State
	rec.Node.State = state
	t.changeState(rec, oldState, state)
	return nil
}

// GetClusterMetrics returns a snapshot of cluster health for the dashboard.
func (t *Tracker) GetClusterMetrics() *types.ClusterMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m := &types.ClusterMetrics{
		Timestamp:    time.Now(),
		ClusterEpoch: atomic.LoadInt64(&t.clusterEpoch),
	}
	for _, rec := range t.nodes {
		m.TotalNodes++
		switch rec.Node.State {
		case types.NodeStateHealthy:
			m.HealthyNodes++
		case types.NodeStateSuspect:
			m.SuspectNodes++
		case types.NodeStateUnavailable, types.NodeStateDecommissioning:
			m.UnavailableNodes++
		}
		m.TotalSizeBytes += rec.Node.DiskTotal - rec.Node.DiskFree
		m.RawCapacity += rec.Node.DiskTotal
		m.FreeCapacity += rec.Node.DiskFree
		m.TotalObjects += rec.Node.ObjectCount
	}
	m.UsableCapacity = m.RawCapacity
	return m
}

// ─── Background Health Check ─────────────────────────────────────────────────

func (t *Tracker) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.checkHeartbeats()
		}
	}
}

func (t *Tracker) checkHeartbeats() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	for _, rec := range t.nodes {
		age := now.Sub(rec.LastHeartbeat)
		oldState := rec.Node.State

		switch {
		case oldState == types.NodeStateDecommissioning || oldState == types.NodeStateRecovering:
			// Leave these states alone
		case age > t.unavailTimeout && oldState != types.NodeStateUnavailable:
			rec.Node.State = types.NodeStateUnavailable
			t.changeState(rec, oldState, types.NodeStateUnavailable)
		case age > t.suspectTimeout && oldState == types.NodeStateHealthy:
			rec.Node.State = types.NodeStateSuspect
			now_ := time.Now()
			rec.SuspectSince = &now_
			t.changeState(rec, oldState, types.NodeStateSuspect)
		}
	}
}

func (t *Tracker) changeState(rec *NodeRecord, oldState, newState types.NodeState) {
	if oldState == newState {
		return
	}
	t.logger.NodeStateChange(rec.Node.ID, string(oldState), string(newState), atomic.LoadInt64(&t.clusterEpoch))
	t.emitEvent("STATE_CHANGE", rec.Node.ID, oldState, newState)
	if t.onStateChange != nil {
		go t.onStateChange(rec.Node.ID, oldState, newState)
	}
}

func (t *Tracker) emitEvent(eventType, nodeID string, oldState, newState types.NodeState) {
	event := MembershipEvent{
		Type:      eventType,
		NodeID:    nodeID,
		OldState:  oldState,
		NewState:  newState,
		Timestamp: time.Now(),
		Epoch:     atomic.LoadInt64(&t.clusterEpoch),
	}
	select {
	case t.eventCh <- event:
	default:
	}
}
