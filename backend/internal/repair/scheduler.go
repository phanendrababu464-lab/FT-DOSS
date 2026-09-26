// Package repair implements the background repair scheduler.
// Detects degraded/corrupt replicas and restores them from healthy sources.
package repair

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
	"github.com/google/uuid"
)

// ─── Scheduler ────────────────────────────────────────────────────────────────

// Repairer is the interface for performing replica repairs.
type Repairer interface {
	RepairReplica(ctx context.Context, bucket, key, versionID, sourceNodeID, targetNodeID string) error
}

// MetadataReader reads object metadata for repair decisions.
type MetadataReader interface {
	GetObjectMeta(bucket, key string) (*types.ObjectMetadata, error)
}

// Scheduler manages the repair job queue and execution.
type Scheduler struct {
	mu          sync.RWMutex
	jobs        map[string]*types.RepairJob
	queue       []*types.RepairJob // priority queue (simple slice for MVP)
	workers     int
	repairer    Repairer
	logger      *logger.Logger
	stopCh      chan struct{}
	jobCh       chan *types.RepairJob
	active      int32 // atomic: number of active repair workers

	// Stats
	totalRepairs    int64
	successRepairs  int64
	failedRepairs   int64
	bytesRepaired   int64
}

// NewScheduler creates a new repair scheduler.
func NewScheduler(workers int, repairer Repairer, log *logger.Logger) *Scheduler {
	return &Scheduler{
		jobs:     make(map[string]*types.RepairJob),
		workers:  workers,
		repairer: repairer,
		logger:   log,
		stopCh:   make(chan struct{}),
		jobCh:    make(chan *types.RepairJob, 1000),
	}
}

// Start begins repair worker goroutines.
func (s *Scheduler) Start() {
	for i := 0; i < s.workers; i++ {
		go s.worker()
	}
}

// Stop halts all repair workers.
func (s *Scheduler) Stop() {
	close(s.stopCh)
}

// ScheduleRepair adds a repair job to the queue.
// Priority 1 = highest (below minimum durability), 5 = lowest.
func (s *Scheduler) ScheduleRepair(
	bucket, key, versionID, sourceNodeID, targetNodeID, reason string,
	priority int,
) *types.RepairJob {
	job := &types.RepairJob{
		ID:         uuid.New().String(),
		Bucket:     bucket,
		Key:        key,
		VersionID:  versionID,
		SourceNode: sourceNodeID,
		TargetNode: targetNodeID,
		State:      types.ReplicaStateMissing,
		Reason:     reason,
		Priority:   priority,
		CreatedAt:  time.Now(),
	}

	s.mu.Lock()
	s.jobs[job.ID] = job
	// Insert in priority order
	inserted := false
	for i, j := range s.queue {
		if priority < j.Priority {
			s.queue = append(s.queue[:i], append([]*types.RepairJob{job}, s.queue[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		s.queue = append(s.queue, job)
	}
	s.mu.Unlock()

	s.logger.Info().
		Str("event", "REPAIR_SCHEDULED").
		Str("job_id", job.ID).
		Str("bucket", bucket).
		Str("key", key).
		Str("version_id", versionID).
		Str("target_node", targetNodeID).
		Str("reason", reason).
		Int("priority", priority).
		Msg("repair job scheduled")

	// Send to worker channel (non-blocking)
	select {
	case s.jobCh <- job:
	default:
	}

	return job
}

// worker processes repair jobs from the queue.
func (s *Scheduler) worker() {
	for {
		select {
		case <-s.stopCh:
			return
		case job := <-s.jobCh:
			atomic.AddInt32(&s.active, 1)
			s.executeRepair(job)
			atomic.AddInt32(&s.active, -1)
		}
	}
}

func (s *Scheduler) executeRepair(job *types.RepairJob) {
	start := time.Now()
	now := time.Now()

	s.mu.Lock()
	job.StartedAt = &now
	job.State = types.ReplicaStateRepairing
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := s.repairer.RepairReplica(ctx, job.Bucket, job.Key, job.VersionID, job.SourceNode, job.TargetNode)

	s.mu.Lock()
	completedAt := time.Now()
	job.CompletedAt = &completedAt
	s.mu.Unlock()

	atomic.AddInt64(&s.totalRepairs, 1)

	if err != nil {
		s.mu.Lock()
		job.State = types.ReplicaStateCorrupt
		job.Error = err.Error()
		s.mu.Unlock()
		atomic.AddInt64(&s.failedRepairs, 1)

		s.logger.Error().
			Err(err).
			Str("event", "REPAIR_FAILED").
			Str("job_id", job.ID).
			Str("bucket", job.Bucket).
			Str("key", job.Key).
			Str("target_node", job.TargetNode).
			Msg("repair failed")
	} else {
		s.mu.Lock()
		job.State = types.ReplicaStateValid
		s.mu.Unlock()
		atomic.AddInt64(&s.successRepairs, 1)

		duration := time.Since(start)
		s.logger.ReplicaRepairCompleted(
			fmt.Sprintf("%s/%s", job.Bucket, job.Key),
			job.VersionID,
			job.TargetNode,
			"",
			float64(duration.Milliseconds()),
		)
	}
}

// GetJobs returns all repair jobs.
func (s *Scheduler) GetJobs() []*types.RepairJob {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.RepairJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		job := *j
		result = append(result, &job)
	}
	return result
}

// GetStats returns repair statistics.
func (s *Scheduler) GetStats() map[string]int64 {
	return map[string]int64{
		"total_repairs":   atomic.LoadInt64(&s.totalRepairs),
		"success_repairs": atomic.LoadInt64(&s.successRepairs),
		"failed_repairs":  atomic.LoadInt64(&s.failedRepairs),
		"queue_depth":     int64(len(s.queue)),
		"active_workers":  int64(atomic.LoadInt32(&s.active)),
	}
}
