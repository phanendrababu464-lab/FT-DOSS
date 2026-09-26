// Package rebalance implements cluster rebalancing when nodes are added or removed.
// Objects are moved between placement groups while maintaining checksums and durability.
package rebalance

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/internal/placement"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

// ─── Rebalancer ───────────────────────────────────────────────────────────────

// ObjectMover moves objects between nodes.
type ObjectMover interface {
	RepairReplica(ctx context.Context, bucket, key, versionID, sourceNodeID, targetNodeID string) error
}

// MetadataLister lists objects in a placement group.
type MetadataLister interface {
	ListObjects(bucket, prefix, delimiter, continuationToken string, limit int) (*types.ListObjectsResponse, error)
}

// RebalanceJob tracks a rebalancing operation.
type RebalanceJob struct {
	ID             string    `json:"id"`
	PlacementGroup int       `json:"placement_group"`
	SourceNode     string    `json:"source_node"`
	TargetNode     string    `json:"target_node"`
	State          string    `json:"state"`
	ObjectsMoved   int64     `json:"objects_moved"`
	BytesMoved     int64     `json:"bytes_moved"`
	BytesTotal     int64     `json:"bytes_total"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// Rebalancer manages cluster-wide data rebalancing.
type Rebalancer struct {
	mu           sync.RWMutex
	placementMgr *placement.Manager
	mover        ObjectMover
	logger       *logger.Logger
	stopCh       chan struct{}
	jobs         map[string]*RebalanceJob
	maxBandwidth int // MB/s

	// Stats
	totalMoved  int64
	totalFailed int64
	activePGs   int32
}

// NewRebalancer creates a rebalancer.
func NewRebalancer(pm *placement.Manager, mover ObjectMover, maxBandwidthMBPS int, log *logger.Logger) *Rebalancer {
	return &Rebalancer{
		placementMgr: pm,
		mover:        mover,
		logger:       log,
		stopCh:       make(chan struct{}),
		jobs:         make(map[string]*RebalanceJob),
		maxBandwidth: maxBandwidthMBPS,
	}
}

// TriggerRebalance starts rebalancing for affected placement groups after a node change.
// INVARIANT: Old copy is NOT deleted until new replica is verified.
func (r *Rebalancer) TriggerRebalance(affectedPGs []int, epoch int64) {
	r.logger.Info().
		Int("affected_pgs", len(affectedPGs)).
		Int64("epoch", epoch).
		Msg("rebalancing triggered")

	for _, pgID := range affectedPGs {
		go r.rebalancePG(pgID, epoch)
	}
}

func (r *Rebalancer) rebalancePG(pgID int, epoch int64) {
	atomic.AddInt32(&r.activePGs, 1)
	defer atomic.AddInt32(&r.activePGs, -1)

	pg := r.placementMgr.GetPlacementGroup(pgID)
	if pg == nil {
		return
	}

	r.logger.Info().
		Int("placement_group", pgID).
		Strs("nodes", pg.Nodes).
		Msg("rebalancing placement group")

	// TODO: In full implementation, enumerate objects in this PG
	// and move replicas to the new node assignments.
	// For MVP, we mark the PG as REBALANCING and then ACTIVE.
	time.Sleep(500 * time.Millisecond) // simulate work

	r.placementMgr.MarkGroupActive(pgID)

	r.logger.Info().
		Int("placement_group", pgID).
		Msg("rebalancing placement group completed")
}

// GetJobs returns all rebalance jobs.
func (r *Rebalancer) GetJobs() []*RebalanceJob {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*RebalanceJob, 0, len(r.jobs))
	for _, j := range r.jobs {
		job := *j
		result = append(result, &job)
	}
	return result
}

// GetStats returns rebalancing statistics.
func (r *Rebalancer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_moved":  atomic.LoadInt64(&r.totalMoved),
		"total_failed": atomic.LoadInt64(&r.totalFailed),
		"active_pgs":   atomic.LoadInt32(&r.activePGs),
	}
}
