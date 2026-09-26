// Package observability implements Prometheus metrics and alerting for FT-DOSS.
package observability

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ft-doss/backend/pkg/types"
)

// ─── Metrics ──────────────────────────────────────────────────────────────────

// Metrics holds all Prometheus metric collectors for FT-DOSS.
type Metrics struct {
	// Availability
	TotalReads       prometheus.Counter
	SuccessfulReads  prometheus.Counter
	TotalWrites      prometheus.Counter
	SuccessfulWrites prometheus.Counter

	// Latency histograms (p50, p95, p99 via Observe)
	ReadLatency  *prometheus.HistogramVec
	WriteLatency *prometheus.HistogramVec
	MetaLatency  *prometheus.HistogramVec
	RepairLatency prometheus.Histogram

	// Durability
	DegradedObjects    prometheus.Gauge
	CorruptReplicas    prometheus.Gauge
	PendingRepairs     prometheus.Gauge
	StaleReplicas      prometheus.Gauge
	DegradedPGs        prometheus.Gauge

	// Capacity
	TotalObjects   prometheus.Gauge
	TotalSizeBytes prometheus.Gauge
	NodeDiskUsage  *prometheus.GaugeVec
	NodeDiskFree   *prometheus.GaugeVec

	// Consensus
	RaftTerm          prometheus.Gauge
	RaftCommitIndex   prometheus.Gauge
	RaftElectionCount prometheus.Counter
	ClusterEpoch      prometheus.Gauge

	// Operations
	ReplicationErrors prometheus.Counter
	ChecksumMismatches prometheus.Counter
	ScrubObjectsChecked prometheus.Counter
	RepairBytesTransferred prometheus.Counter

	// Node states
	NodesByState *prometheus.GaugeVec
}

var (
	metricsOnce sync.Once
	globalMetrics *Metrics
)

// New creates and registers all Prometheus metrics.
func New(nodeID string) *Metrics {
	metricsOnce.Do(func() {
		labels := prometheus.Labels{"node_id": nodeID}
		_ = labels

		m := &Metrics{}

	m.TotalReads = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "reads_total",
		Help:      "Total number of object read attempts",
	})

	m.SuccessfulReads = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "reads_successful_total",
		Help:      "Total number of successful object reads",
	})

	m.TotalWrites = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "writes_total",
		Help:      "Total number of object write attempts",
	})

	m.SuccessfulWrites = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "writes_successful_total",
		Help:      "Total number of successful quorum writes",
	})

	m.ReadLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ftdoss",
		Name:      "read_latency_ms",
		Help:      "Object read latency in milliseconds",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
	}, []string{"status"})

	m.WriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ftdoss",
		Name:      "write_latency_ms",
		Help:      "Object write latency in milliseconds",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
	}, []string{"status"})

	m.MetaLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ftdoss",
		Name:      "metadata_latency_ms",
		Help:      "Metadata operation latency in milliseconds",
		Buckets:   []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250},
	}, []string{"operation"})

	m.RepairLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ftdoss",
		Name:      "repair_latency_ms",
		Help:      "Replica repair latency in milliseconds",
		Buckets:   []float64{100, 500, 1000, 5000, 10000, 30000},
	})

	m.DegradedObjects = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "degraded_objects",
		Help:      "Number of objects below desired replica count",
	})

	m.CorruptReplicas = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "corrupt_replicas",
		Help:      "Number of known corrupt replicas",
	})

	m.PendingRepairs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "pending_repairs",
		Help:      "Number of repair jobs in queue",
	})

	m.StaleReplicas = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "stale_replicas",
		Help:      "Number of stale replicas (wrong version)",
	})

	m.DegradedPGs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "degraded_placement_groups",
		Help:      "Number of placement groups in degraded state",
	})

	m.TotalObjects = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "total_objects",
		Help:      "Total number of objects in the cluster",
	})

	m.TotalSizeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "total_size_bytes",
		Help:      "Total bytes stored in the cluster",
	})

	m.NodeDiskUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "node_disk_used_bytes",
		Help:      "Disk bytes used per node",
	}, []string{"node_id"})

	m.NodeDiskFree = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "node_disk_free_bytes",
		Help:      "Disk bytes free per node",
	}, []string{"node_id"})

	m.RaftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "raft_term",
		Help:      "Current Raft term",
	})

	m.RaftCommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "raft_commit_index",
		Help:      "Raft commit index",
	})

	m.RaftElectionCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "raft_elections_total",
		Help:      "Total number of Raft leader elections",
	})

	m.ClusterEpoch = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "cluster_epoch",
		Help:      "Current cluster epoch (increments on membership change)",
	})

	m.ReplicationErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "replication_errors_total",
		Help:      "Total number of replication errors",
	})

	m.ChecksumMismatches = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "checksum_mismatches_total",
		Help:      "Total checksum mismatches detected",
	})

	m.ScrubObjectsChecked = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "scrub_objects_checked_total",
		Help:      "Total objects checked by scrubber",
	})

	m.RepairBytesTransferred = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ftdoss",
		Name:      "repair_bytes_transferred_total",
		Help:      "Total bytes transferred during replica repairs",
	})

	m.NodesByState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ftdoss",
		Name:      "nodes_by_state",
		Help:      "Number of nodes per state",
	}, []string{"state"})

	globalMetrics = m
	})
	return globalMetrics
}

// Handler returns the Prometheus HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

// ─── Alert Manager ────────────────────────────────────────────────────────────

// AlertManager manages system alerts.
type AlertManager struct {
	mu     sync.RWMutex
	alerts map[string]*types.Alert
}

// NewAlertManager creates a new alert manager.
func NewAlertManager() *AlertManager {
	return &AlertManager{
		alerts: make(map[string]*types.Alert),
	}
}

// FireAlert creates or updates an alert.
func (am *AlertManager) FireAlert(id string, severity types.AlertSeverity, title, message, nodeID string) *types.Alert {
	am.mu.Lock()
	defer am.mu.Unlock()

	alert := &types.Alert{
		ID:        id,
		Severity:  severity,
		Title:     title,
		Message:   message,
		NodeID:    nodeID,
		CreatedAt: time.Now(),
		IsActive:  true,
	}
	am.alerts[id] = alert
	return alert
}

// ResolveAlert marks an alert as resolved.
func (am *AlertManager) ResolveAlert(id string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if alert, ok := am.alerts[id]; ok {
		now := time.Now()
		alert.IsActive = false
		alert.ResolvedAt = &now
	}
}

// GetActiveAlerts returns all active alerts.
func (am *AlertManager) GetActiveAlerts() []*types.Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	var result []*types.Alert
	for _, a := range am.alerts {
		if a.IsActive {
			alert := *a
			result = append(result, &alert)
		}
	}
	return result
}

// GetAllAlerts returns all alerts (including resolved).
func (am *AlertManager) GetAllAlerts() []*types.Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	result := make([]*types.Alert, 0, len(am.alerts))
	for _, a := range am.alerts {
		alert := *a
		result = append(result, &alert)
	}
	return result
}

// CheckRules evaluates alert rules against cluster metrics.
func (am *AlertManager) CheckRules(metrics *types.ClusterMetrics) {
	// CRITICAL: valid replicas below minimum
	if metrics.DegradedObjects > 0 {
		am.FireAlert(
			"degraded-objects",
			types.AlertCritical,
			"Objects Below Minimum Durability",
			fmt.Sprintf("%d objects have fewer replicas than required", metrics.DegradedObjects),
			"",
		)
	} else {
		am.ResolveAlert("degraded-objects")
	}

	// CRITICAL: corrupt replicas
	if metrics.CorruptReplicas > 0 {
		am.FireAlert(
			"corrupt-replicas",
			types.AlertCritical,
			"Corrupt Replicas Detected",
			fmt.Sprintf("%d corrupt replicas require immediate repair", metrics.CorruptReplicas),
			"",
		)
	} else {
		am.ResolveAlert("corrupt-replicas")
	}

	// WARNING: nodes unavailable
	if metrics.UnavailableNodes > 0 {
		am.FireAlert(
			"nodes-unavailable",
			types.AlertWarning,
			"Storage Nodes Unavailable",
			fmt.Sprintf("%d storage nodes are currently unavailable", metrics.UnavailableNodes),
			"",
		)
	} else {
		am.ResolveAlert("nodes-unavailable")
	}

	// WARNING: large repair backlog
	if metrics.PendingRepairs > 100 {
		am.FireAlert(
			"repair-backlog",
			types.AlertWarning,
			"Large Repair Backlog",
			fmt.Sprintf("%d repairs pending", metrics.PendingRepairs),
			"",
		)
	} else {
		am.ResolveAlert("repair-backlog")
	}
}
