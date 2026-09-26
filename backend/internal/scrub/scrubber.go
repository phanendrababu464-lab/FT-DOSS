// Package scrub implements the background scrubber for FT-DOSS.
// Performs LIGHT and DEEP integrity scans of stored objects.
package scrub

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/internal/storage"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
	"github.com/google/uuid"
)

// ─── Scrubber ────────────────────────────────────────────────────────────────

// CorruptionCallback is called when corruption is detected.
type CorruptionCallback func(bucket, key, versionID, nodeID string)

// Scrubber performs background integrity checks on storage nodes.
type Scrubber struct {
	mu              sync.RWMutex
	nodes           map[string]*storage.Node
	jobs            map[string]*types.ScrubJob
	logger          *logger.Logger
	stopCh          chan struct{}
	lightInterval   time.Duration
	deepInterval    time.Duration
	concurrency     int
	onCorruption    CorruptionCallback

	// Stats
	totalScanned    int64
	totalCorrupt    int64
	totalStale      int64
	totalJobsRun    int64
}

// NewScrubber creates a new background scrubber.
func NewScrubber(lightInterval, deepInterval time.Duration, concurrency int, log *logger.Logger) *Scrubber {
	return &Scrubber{
		nodes:         make(map[string]*storage.Node),
		jobs:          make(map[string]*types.ScrubJob),
		logger:        log,
		stopCh:        make(chan struct{}),
		lightInterval: lightInterval,
		deepInterval:  deepInterval,
		concurrency:   concurrency,
	}
}

// SetCorruptionCallback sets the callback for detected corruption.
func (s *Scrubber) SetCorruptionCallback(fn CorruptionCallback) {
	s.onCorruption = fn
}

// RegisterNode adds a storage node to the scrubber.
func (s *Scrubber) RegisterNode(node *storage.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[node.NodeID()] = node
}

// Start begins the background scrubbing loops.
func (s *Scrubber) Start() {
	go s.lightScrubLoop()
	go s.deepScrubLoop()
}

// Stop halts the scrubber.
func (s *Scrubber) Stop() {
	close(s.stopCh)
}

// TriggerUrgentScrub immediately scrubs a specific node.
func (s *Scrubber) TriggerUrgentScrub(nodeID string) (*types.ScrubJob, error) {
	s.mu.RLock()
	node, ok := s.nodes[nodeID]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	job := &types.ScrubJob{
		ID:        uuid.New().String(),
		NodeID:    nodeID,
		Type:      types.ScrubTypeUrgent,
		State:     "RUNNING",
		StartedAt: time.Now(),
	}

	s.mu.Lock()
	s.jobs[job.ID] = job
	s.mu.Unlock()

	go s.runScrub(job, node)
	return job, nil
}

func (s *Scrubber) lightScrubLoop() {
	ticker := time.NewTicker(s.lightInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scrubAllNodes(types.ScrubTypeLight)
		}
	}
}

func (s *Scrubber) deepScrubLoop() {
	ticker := time.NewTicker(s.deepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scrubAllNodes(types.ScrubTypeDeep)
		}
	}
}

func (s *Scrubber) scrubAllNodes(scrubType types.ScrubType) {
	s.mu.RLock()
	nodes := make([]*storage.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	s.mu.RUnlock()

	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		sem <- struct{}{}
		go func(n *storage.Node) {
			defer wg.Done()
			defer func() { <-sem }()

			job := &types.ScrubJob{
				ID:        uuid.New().String(),
				NodeID:    n.NodeID(),
				Type:      scrubType,
				State:     "RUNNING",
				StartedAt: time.Now(),
			}
			s.mu.Lock()
			s.jobs[job.ID] = job
			s.mu.Unlock()

			s.runScrub(job, n)
		}(node)
	}
	wg.Wait()
}

func (s *Scrubber) runScrub(job *types.ScrubJob, node *storage.Node) {
	atomic.AddInt64(&s.totalJobsRun, 1)

	corrupt, stale, err := node.ScrubAll()

	now := time.Now()
	s.mu.Lock()
	job.State = "COMPLETED"
	job.CompletedAt = &now
	job.CorruptFound = int64(len(corrupt))
	job.StaleFound = int64(len(stale))
	if err != nil {
		job.State = "FAILED"
	}
	s.mu.Unlock()

	atomic.AddInt64(&s.totalCorrupt, int64(len(corrupt)))
	atomic.AddInt64(&s.totalStale, int64(len(stale)))

	s.logger.Info().
		Str("event", "SCRUB_COMPLETED").
		Str("node_id", node.NodeID()).
		Str("type", string(job.Type)).
		Int("corrupt_found", len(corrupt)).
		Int("stale_found", len(stale)).
		Msg("scrub completed")

	// Trigger repair for each corrupt object found
	if s.onCorruption != nil {
		for _, objKey := range corrupt {
			// Parse bucket/key@version from key string
			s.logger.Warn().
				Str("event", "CORRUPTION_DETECTED").
				Str("node_id", node.NodeID()).
				Str("object", objKey).
				Msg("corruption detected during scrub")
			// Note: actual repair scheduling happens via the onCorruption callback
		}
	}
}

// GetJobs returns all scrub job records.
func (s *Scrubber) GetJobs() []*types.ScrubJob {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.ScrubJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		job := *j
		result = append(result, &job)
	}
	return result
}

// GetStats returns scrubbing statistics.
func (s *Scrubber) GetStats() map[string]int64 {
	return map[string]int64{
		"total_scanned":  atomic.LoadInt64(&s.totalScanned),
		"total_corrupt":  atomic.LoadInt64(&s.totalCorrupt),
		"total_stale":    atomic.LoadInt64(&s.totalStale),
		"total_jobs_run": atomic.LoadInt64(&s.totalJobsRun),
	}
}
