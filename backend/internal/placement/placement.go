// Package placement implements deterministic object placement across storage nodes.
// Objects are mapped: bucket+key -> placement group -> storage nodes.
// This avoids a central object-location table by making placement computable.
package placement

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/ft-doss/backend/pkg/types"
)

const defaultPlacementGroupCount = 1024

// Manager handles placement group computation and cluster mapping.
type Manager struct {
	mu              sync.RWMutex
	placementGroups int
	nodes           []*types.StorageNode
	groups          []types.PlacementGroup
	clusterEpoch    int64
	failureDomain   string // rack, zone, region
}

// NewManager creates a new placement manager.
func NewManager(pgCount int, failureDomain string) *Manager {
	if pgCount <= 0 {
		pgCount = defaultPlacementGroupCount
	}
	m := &Manager{
		placementGroups: pgCount,
		failureDomain:   failureDomain,
		groups:          make([]types.PlacementGroup, pgCount),
	}
	// Initialize empty placement groups
	for i := 0; i < pgCount; i++ {
		m.groups[i] = types.PlacementGroup{
			ID:    i,
			State: "ACTIVE",
			Nodes: []string{},
		}
	}
	return m
}

// PlacementGroupID computes the placement group ID for a given bucket and key.
// This is deterministic: same inputs always produce the same group.
func (m *Manager) PlacementGroupID(bucket, key string) int {
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%s/%s", bucket, key)
	return int(h.Sum32()) % m.placementGroups
}

// GetReplicas returns the ordered list of storage nodes for an object.
// Primary is index 0, secondaries follow.
func (m *Manager) GetReplicas(bucket, key string, replicationFactor int) ([]string, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pgID := m.PlacementGroupID(bucket, key)
	pg := m.groups[pgID]

	if len(pg.Nodes) == 0 {
		// Compute from current healthy nodes
		nodes := m.selectNodesForGroup(pgID, replicationFactor)
		return nodes, pgID, nil
	}

	return pg.Nodes, pgID, nil
}

// UpdateCluster recalculates all placement groups based on new membership.
// Only affected groups are moved (minimizes data movement).
func (m *Manager) UpdateCluster(nodes []*types.StorageNode, epoch int64) []int {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes = nodes
	m.clusterEpoch = epoch

	// Identify affected placement groups
	var affected []int
	for i := 0; i < m.placementGroups; i++ {
		oldNodes := m.groups[i].Nodes
		newNodes := m.selectNodesForGroup(i, 3) // default RF=3
		if !nodesEqual(oldNodes, newNodes) {
			m.groups[i].Nodes = newNodes
			m.groups[i].State = "REBALANCING"
			affected = append(affected, i)
		}
	}

	return affected
}

// selectNodesForGroup picks storage nodes for a placement group.
// Uses consistent hashing to minimize movement when nodes are added/removed.
// Respects failure domain constraints (no two replicas on same rack/zone).
func (m *Manager) selectNodesForGroup(pgID, replicationFactor int) []string {
	// Only healthy/recovering nodes are eligible
	var eligible []*types.StorageNode
	for _, n := range m.nodes {
		if n.State == types.NodeStateHealthy || n.State == types.NodeStateRecovering {
			eligible = append(eligible, n)
		}
	}

	if len(eligible) == 0 {
		return nil
	}

	// Sort for determinism
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].ID < eligible[j].ID
	})

	// Score each node for this PG using consistent hashing
	type scored struct {
		node  *types.StorageNode
		score uint32
	}
	var scored_nodes []scored
	for _, n := range eligible {
		h := fnv.New32a()
		_, _ = fmt.Fprintf(h, "%s-%d-%d", n.ID, pgID, m.clusterEpoch)
		scored_nodes = append(scored_nodes, struct {
			node  *types.StorageNode
			score uint32
		}{n, h.Sum32()})
	}
	sort.Slice(scored_nodes, func(i, j int) bool {
		return scored_nodes[i].score < scored_nodes[j].score
	})

	// Select up to replicationFactor nodes, respecting failure domain
	var selected []string
	usedDomains := map[string]bool{}

	for _, sn := range scored_nodes {
		if len(selected) >= replicationFactor {
			break
		}
		domain := m.failureDomainKey(sn.node)
		if m.failureDomain != "" && usedDomains[domain] {
			continue // skip: same failure domain
		}
		selected = append(selected, sn.node.ID)
		usedDomains[domain] = true
	}

	// If not enough unique failure domains, relax constraint
	if len(selected) < replicationFactor && len(selected) < len(eligible) {
		for _, sn := range scored_nodes {
			if len(selected) >= replicationFactor {
				break
			}
			found := false
			for _, s := range selected {
				if s == sn.node.ID {
					found = true
					break
				}
			}
			if !found {
				selected = append(selected, sn.node.ID)
			}
		}
	}

	return selected
}

// failureDomainKey returns the failure domain identifier for a node.
func (m *Manager) failureDomainKey(n *types.StorageNode) string {
	switch m.failureDomain {
	case "region":
		return n.Region
	case "zone":
		return n.Zone
	case "rack":
		return fmt.Sprintf("%s/%s", n.Zone, n.Rack)
	default:
		return fmt.Sprintf("%s/%s/%s", n.Region, n.Zone, n.Rack)
	}
}

// GetPlacementGroup returns the placement group state for a given ID.
func (m *Manager) GetPlacementGroup(pgID int) *types.PlacementGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pgID < 0 || pgID >= m.placementGroups {
		return nil
	}
	pg := m.groups[pgID]
	return &pg
}

// GetAllGroups returns all placement groups (for dashboard).
func (m *Manager) GetAllGroups() []types.PlacementGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]types.PlacementGroup, len(m.groups))
	copy(result, m.groups)
	return result
}

// MarkGroupActive marks a placement group as active after rebalancing.
func (m *Manager) MarkGroupActive(pgID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pgID >= 0 && pgID < m.placementGroups {
		m.groups[pgID].State = "ACTIVE"
	}
}

// AddNode registers a new node and recalculates affected placement groups.
func (m *Manager) AddNode(node *types.StorageNode, epoch int64) []int {
	m.mu.Lock()
	for _, n := range m.nodes {
		if n.ID == node.ID {
			m.mu.Unlock()
			return nil // already exists
		}
	}
	m.nodes = append(m.nodes, node)
	m.mu.Unlock()
	return m.UpdateCluster(m.getNodes(), epoch)
}

// RemoveNode deregisters a node and marks affected placement groups.
func (m *Manager) RemoveNode(nodeID string, epoch int64) []int {
	m.mu.Lock()
	var newNodes []*types.StorageNode
	for _, n := range m.nodes {
		if n.ID != nodeID {
			newNodes = append(newNodes, n)
		}
	}
	m.nodes = newNodes
	m.mu.Unlock()
	return m.UpdateCluster(newNodes, epoch)
}

func (m *Manager) getNodes() []*types.StorageNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*types.StorageNode, len(m.nodes))
	copy(result, m.nodes)
	return result
}

// GetNodes returns the current list of nodes.
func (m *Manager) GetNodes() []*types.StorageNode {
	return m.getNodes()
}

// nodesEqual compares two node lists for equality.
func nodesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
