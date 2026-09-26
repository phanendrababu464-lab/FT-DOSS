// Package storage - registry.go manages physical storage nodes.
package storage

import (
	"path/filepath"
	"sync"

	"github.com/ft-doss/backend/pkg/logger"
)

// Registry manages multiple physical storage nodes on disk.
type Registry struct {
	mu             sync.RWMutex
	nodes          map[string]*Node
	baseDataDir    string
	baseWalDir     string
	fencingEnabled bool
	logger         *logger.Logger
}

// NewRegistry creates a new storage node registry.
func NewRegistry(baseDataDir, baseWalDir string, fencingEnabled bool, log *logger.Logger) *Registry {
	return &Registry{
		nodes:          make(map[string]*Node),
		baseDataDir:    baseDataDir,
		baseWalDir:     baseWalDir,
		fencingEnabled: fencingEnabled,
		logger:         log,
	}
}

// GetOrCreateNode returns an existing node or initializes a new physical storage node.
func (r *Registry) GetOrCreateNode(nodeID string) (*Node, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if n, ok := r.nodes[nodeID]; ok {
		return n, nil
	}

	dataDir := filepath.Join(r.baseDataDir, nodeID)
	walDir := filepath.Join(r.baseWalDir, nodeID)

	node, err := NewNode(nodeID, dataDir, walDir, r.fencingEnabled, r.logger)
	if err != nil {
		return nil, err
	}

	r.nodes[nodeID] = node
	return node, nil
}

// GetNode returns the node if it exists, or nil.
func (r *Registry) GetNode(nodeID string) *Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeID]
}

// GetAllNodes returns all managed nodes in the registry.
func (r *Registry) GetAllNodes() map[string]*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*Node, len(r.nodes))
	for k, v := range r.nodes {
		result[k] = v
	}
	return result
}

// RemoveNode removes a node from the registry.
func (r *Registry) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
}

// ResetAll clears stored data for all registered nodes.
func (r *Registry) ResetAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, n := range r.nodes {
		n.ResetStorage()
	}
}
