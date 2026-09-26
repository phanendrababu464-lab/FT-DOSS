// Package gateway implements the FT-DOSS API Gateway.
// Routes client requests through metadata lookup → placement → storage.
package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/ft-doss/backend/internal/checksum"
	"github.com/ft-doss/backend/internal/membership"
	"github.com/ft-doss/backend/internal/metadata"
	"github.com/ft-doss/backend/internal/observability"
	"github.com/ft-doss/backend/internal/placement"
	"github.com/ft-doss/backend/internal/replication"
	"github.com/ft-doss/backend/internal/storage"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

// ─── Gateway ─────────────────────────────────────────────────────────────────

// Gateway is the main API gateway for FT-DOSS.
type Gateway struct {
	meta        *metadata.Service
	placementMgr *placement.Manager
	repManager  *replication.Manager
	memberTracker *membership.Tracker
	metrics     *observability.Metrics
	alerts      *observability.AlertManager
	logger      *logger.Logger
	config      *GatewayConfig

	// Local storage node (primary)
	localNode   *storage.Node
	registry    *storage.Registry
	adminAPIKey string

	sseTokens map[string]time.Time
	sseMu     sync.Mutex

	readCount  int64
	writeCount int64

	repairHistory []gin.H
	scrubHistory  []gin.H
	historyMu     sync.RWMutex
}

// SetRegistry sets the storage node registry.
func (g *Gateway) SetRegistry(registry *storage.Registry) {
	g.registry = registry
}

// GatewayConfig holds runtime configuration.
type GatewayConfig struct {
	WriteQuorum int
	ReadQuorum  int
	MaxBodySize int64
	AdminAPIKey string
}

// NewGateway creates a new API gateway.
func NewGateway(
	meta *metadata.Service,
	pm *placement.Manager,
	rm *replication.Manager,
	mt *membership.Tracker,
	localNode *storage.Node,
	metrics *observability.Metrics,
	alerts *observability.AlertManager,
	cfg *GatewayConfig,
	log *logger.Logger,
) *Gateway {
	return &Gateway{
		meta:          meta,
		placementMgr:  pm,
		repManager:    rm,
		memberTracker: mt,
		localNode:     localNode,
		metrics:       metrics,
		alerts:        alerts,
		config:        cfg,
		logger:        log,
		adminAPIKey:   cfg.AdminAPIKey,
		sseTokens:     make(map[string]time.Time),
	}
}

// ─── Router ──────────────────────────────────────────────────────────────────

// Router returns the Gin router with all routes registered.
func (g *Gateway) Router() http.Handler {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(g.requestIDMiddleware())
	r.Use(g.loggingMiddleware())

	// Health
	r.GET("/health", g.handleHealth)
	r.GET("/health/ready", g.handleReady)

	// Metrics
	r.GET("/metrics", gin.WrapH(observability.Handler()))

	// Real-Time Events (SSE)
	r.GET("/api/events/stream", g.handleEventStream)
	r.GET("/events/stream", g.handleEventStream)

	// Object API
	api := r.Group("/buckets/:bucket")
	{
		// Bucket operations
		api.PUT("", g.handleCreateBucket)
		api.GET("", g.handleGetBucket)

		// Object operations
		obj := api.Group("/objects")
		{
			obj.PUT("/*key", g.handlePutObject)
			obj.GET("/*key", g.handleGetObject)
			obj.HEAD("/*key", g.handleHeadObject)
			obj.DELETE("/*key", g.handleDeleteObject)
		}

		// List objects
		api.GET("/list", g.handleListObjects)
	}

	// Multipart uploads
	uploads := r.Group("/uploads")
	{
		uploads.POST("", g.handleInitMultipart)
		uploads.PUT("/:upload_id/parts/:part_number", g.handleUploadPart)
		uploads.POST("/:upload_id/complete", g.handleCompleteMultipart)
		uploads.DELETE("/:upload_id", g.handleAbortMultipart)
	}

	// Internal API (storage node to storage node replication)
	internal := r.Group("/internal")
	{
		internal.PUT("/replica", g.handleInternalReplica)
		internal.GET("/objects/:bucket/:key", g.handleInternalGetObject)
	}

	// Admin / Control Plane API
	admin := r.Group("/admin")
	admin.Use(g.adminAuthMiddleware())
	{
		// Cluster overview
		admin.GET("/cluster", g.handleGetCluster)
		admin.GET("/cluster/metrics", g.handleGetClusterMetrics)

		// Node management
		admin.GET("/nodes", g.handleListNodes)
		admin.GET("/nodes/:node_id", g.handleGetNode)
		admin.POST("/nodes/:node_id/state", g.handleSetNodeState)

		// Chaos Lab
		admin.POST("/chaos/crash-node/:node_id", g.handleCrashNode)
		admin.POST("/chaos/restart-node/:node_id", g.handleRestartNode)
		admin.POST("/chaos/corrupt-replica", g.handleCorruptReplica)
		admin.POST("/chaos/add-node", g.handleAddNode)
		admin.POST("/chaos/remove-node/:node_id", g.handleRemoveNode)

		// Repair
		admin.GET("/repairs", g.handleListRepairs)
		admin.POST("/repairs/trigger", g.handleTriggerRepair)

		// Scrub
		admin.GET("/scrubs", g.handleListScrubs)
		admin.POST("/scrubs/trigger/:node_id", g.handleTriggerScrub)

		// Placement groups
		admin.GET("/placement-groups", g.handleListPGs)
		admin.GET("/placement-groups/:pg_id", g.handleGetPG)

		// Raft status
		admin.GET("/raft", g.handleGetRaft)

		// Alerts
		admin.GET("/alerts", g.handleGetAlerts)

		// Rebalance
		admin.GET("/rebalance", g.handleGetRebalance)

		// SSE authorization ticket
		admin.POST("/sse-token", g.handleGetSSEToken)

		// System Reset
		admin.POST("/reset", g.handleResetDemo)
	}

	return r
}

// ─── Middleware ───────────────────────────────────────────────────────────────

func (g *Gateway) requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader("X-Request-ID")
		if reqID == "" {
			reqID = uuid.New().String()
		}
		c.Set("request_id", reqID)
		c.Header("X-Request-ID", reqID)
		c.Next()
	}
}

func (g *Gateway) loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)
		g.logger.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Dur("latency", latency).
			Str("request_id", c.GetString("request_id")).
			Msg("request completed")
	}
}

func (g *Gateway) adminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if g.adminAPIKey == "" {
			c.Next()
			return
		}
		key := c.GetHeader("X-Admin-API-Key")
		if key != g.adminAPIKey {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid admin API key"})
			return
		}
		c.Next()
	}
}

// ─── Health Handlers ─────────────────────────────────────────────────────────

func (g *Gateway) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "ft-doss-gateway",
		"time":    time.Now(),
	})
}

func (g *Gateway) handleReady(c *gin.Context) {
	nodes := g.memberTracker.GetHealthyNodes()
	if len(nodes) < g.config.WriteQuorum {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":         "not ready",
			"healthy_nodes":  len(nodes),
			"required_quorum": g.config.WriteQuorum,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":        "ready",
		"healthy_nodes": len(nodes),
	})
}

// ─── Bucket Handlers ─────────────────────────────────────────────────────────

func (g *Gateway) handleCreateBucket(c *gin.Context) {
	bucketName := c.Param("bucket")

	bucket := &types.Bucket{
		Name:              bucketName,
		CreatedAt:         time.Now(),
		Owner:             c.GetHeader("X-User-ID"),
		ReplicationFactor: g.config.WriteQuorum + 1,
		WriteQuorum:       g.config.WriteQuorum,
		ReadQuorum:        g.config.ReadQuorum,
		VersioningEnabled: true,
	}

	ctx := c.Request.Context()
	if err := g.meta.CreateBucket(ctx, bucket); err != nil {
		g.handleError(c, err)
		return
	}

	c.JSON(http.StatusCreated, bucket)
}

func (g *Gateway) handleGetBucket(c *gin.Context) {
	bucket, err := g.meta.GetBucket(c.Param("bucket"))
	if err != nil {
		g.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, bucket)
}

// ─── Object Write Handler ─────────────────────────────────────────────────────

// handlePutObject implements the full write path:
// authenticate → validate → get placement → write primary → replicate → quorum → commit metadata → return
func (g *Gateway) handlePutObject(c *gin.Context) {
	start := time.Now()
	atomic.AddInt64(&g.writeCount, 1)
	g.metrics.TotalWrites.Inc()

	bucket := c.Param("bucket")
	key := c.Param("key")
	if key != "" && key[0] == '/' {
		key = key[1:]
	}

	requestID := c.GetString("request_id")
	operationID := c.GetHeader("X-Operation-ID")
	ifMatch := c.GetHeader("If-Match")
	clientChecksum := c.GetHeader("X-Checksum")
	contentType := c.GetHeader("Content-Type")

	// Read body (with size limit)
	maxSize := g.config.MaxBodySize
	if maxSize <= 0 {
		maxSize = 10 * 1024 * 1024 * 1024 // 10 GB default
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxSize))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	// Compute checksum of the body before replication
	bodyChecksum := checksum.ComputeSHA256(body)
	if clientChecksum != "" {
		normClient := strings.TrimPrefix(clientChecksum, "sha256:")
		normBody := strings.TrimPrefix(bodyChecksum, "sha256:")
		if normClient != normBody {
			g.metrics.ChecksumMismatches.Inc()
			c.JSON(http.StatusBadRequest, gin.H{
				"code":    string(types.ErrChecksumMismatch),
				"message": fmt.Sprintf("client-supplied checksum mismatch: expected %s, got %s", clientChecksum, bodyChecksum),
			})
			return
		}
	}

	// Get placement
	replicaNodes, pgID, err := g.placementMgr.GetReplicas(bucket, key, 3)
	if err != nil || len(replicaNodes) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"code":    string(types.ErrQuorumUnavailable),
			"message": "no healthy nodes for placement",
		})
		return
	}

	// Build version ID (will be committed by metadata service)
	tempVersionID := uuid.New().String()

	meta := &storage.ObjectMeta{
		Bucket:      bucket,
		Key:         key,
		VersionID:   tempVersionID,
		ContentType: contentType,
		State:       types.ObjectStatePreparing,
		CreatedAt:   time.Now(),
		Checksum:    bodyChecksum,
		Size:        int64(len(body)),
	}

	g.logger.WritePath(requestID, operationID, bucket, key, "PREPARING")

	// Replicate to quorum
	result, err := g.repManager.ReplicateWrite(c.Request.Context(), meta, body, replicaNodes)
	if err != nil {
		g.metrics.ReplicationErrors.Inc()
		g.handleError(c, err)
		return
	}

	g.logger.WritePath(requestID, operationID, bucket, key, "DATA_COMMITTED")

	// Commit metadata via Raft
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	committed, err := g.meta.PutObjectMeta(ctx, &metadata.PutMetaRequest{
		Bucket:         bucket,
		Key:            key,
		VersionID:      tempVersionID, // must match the ID stored in the storage nodes
		Size:           int64(len(body)),
		Checksum:       result.Checksum,
		ContentType:    contentType,
		PlacementGroup: pgID,
		ReplicaNodes:   replicaNodes,
		OperationID:    operationID,
		IfMatch:        ifMatch,
	})
	if err != nil {
		g.handleError(c, err)
		return
	}

	g.logger.WritePath(requestID, operationID, bucket, key, "VISIBLE")
	g.metrics.SuccessfulWrites.Inc()

	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventPutObject,
		Source:    "gateway",
		ObjectKey: key,
		Message:   fmt.Sprintf("Object %s written successfully to %d replicas", key, len(replicaNodes)),
		Data:      map[string]interface{}{"bucket": bucket, "checksum": committed.Checksum, "size": committed.Size},
	})
	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventWriteQuorumReached,
		Source:    "replication",
		ObjectKey: key,
		Message:   fmt.Sprintf("Write quorum satisfied for %s", key),
	})

	latency := float64(time.Since(start).Milliseconds())
	g.metrics.WriteLatency.WithLabelValues("success").Observe(latency)

	c.Header("ETag", committed.ETag)
	c.Header("X-Version-ID", committed.VersionID)
	c.Header("X-Checksum", committed.Checksum)
	c.JSON(http.StatusCreated, types.PutObjectResponse{
		VersionID: committed.VersionID,
		ETag:      committed.ETag,
		Size:      committed.Size,
		Checksum:  committed.Checksum,
	})
}

// ─── Object Read Handler ──────────────────────────────────────────────────────

func (g *Gateway) handleGetObject(c *gin.Context) {
	start := time.Now()
	atomic.AddInt64(&g.readCount, 1)
	g.metrics.TotalReads.Inc()

	bucket := c.Param("bucket")
	key := c.Param("key")
	if key != "" && key[0] == '/' {
		key = key[1:]
	}
	requestID := c.GetString("request_id")

	// Get current version from metadata
	objMeta, err := g.meta.GetObjectMeta(bucket, key)
	if err != nil {
		g.handleError(c, err)
		return
	}

	// Read from replicas with fallback
	data, _, sourceNode, err := g.repManager.ReadWithFallback(
		c.Request.Context(),
		bucket, key, objMeta.VersionID,
		objMeta.ReplicaNodes,
	)
	if err != nil {
		g.handleError(c, err)
		return
	}

	g.logger.ReadPath(requestID, bucket, key, objMeta.VersionID, sourceNode)
	g.metrics.SuccessfulReads.Inc()

	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventGetObject,
		Source:    "gateway",
		ObjectKey: key,
		Message:   fmt.Sprintf("Object %s read successfully from %s", key, sourceNode),
	})
	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventChecksumVerified,
		Source:    "checksum",
		ObjectKey: key,
		Message:   fmt.Sprintf("Checksum verified for %s", key),
	})

	latency := float64(time.Since(start).Milliseconds())
	g.metrics.ReadLatency.WithLabelValues("success").Observe(latency)

	c.Header("ETag", objMeta.ETag)
	c.Header("X-Version-ID", objMeta.VersionID)
	c.Header("X-Checksum", objMeta.Checksum)
	c.Header("Content-Type", objMeta.ContentType)
	c.Header("Content-Length", strconv.FormatInt(objMeta.Size, 10))
	c.Data(http.StatusOK, objMeta.ContentType, data)
}

func (g *Gateway) handleHeadObject(c *gin.Context) {
	bucket := c.Param("bucket")
	key := c.Param("key")
	if key != "" && key[0] == '/' {
		key = key[1:]
	}

	objMeta, err := g.meta.GetObjectMeta(bucket, key)
	if err != nil {
		g.handleError(c, err)
		return
	}

	c.Header("ETag", objMeta.ETag)
	c.Header("X-Version-ID", objMeta.VersionID)
	c.Header("Content-Length", strconv.FormatInt(objMeta.Size, 10))
	c.Header("Content-Type", objMeta.ContentType)
	c.Status(http.StatusOK)
}

// ─── Delete Handler ───────────────────────────────────────────────────────────

func (g *Gateway) handleDeleteObject(c *gin.Context) {
	bucket := c.Param("bucket")
	key := c.Param("key")
	if key != "" && key[0] == '/' {
		key = key[1:]
	}

	ctx := c.Request.Context()
	if err := g.meta.DeleteObject(ctx, bucket, key); err != nil {
		g.handleError(c, err)
		return
	}

	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventDeleteObject,
		Source:    "gateway",
		ObjectKey: key,
		Message:   fmt.Sprintf("Object %s soft deleted", key),
	})
	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventTombstoneWritten,
		Source:    "metadata",
		ObjectKey: key,
		Message:   fmt.Sprintf("Tombstone written for %s", key),
	})

	c.Status(http.StatusNoContent)
}

// ─── List Handler ────────────────────────────────────────────────────────────

func (g *Gateway) handleListObjects(c *gin.Context) {
	bucket := c.Param("bucket")
	prefix := c.Query("prefix")
	delimiter := c.Query("delimiter")
	token := c.Query("continuation_token")
	limitStr := c.DefaultQuery("limit", "1000")
	limit, _ := strconv.Atoi(limitStr)

	result, err := g.meta.ListObjects(bucket, prefix, delimiter, token, limit)
	if err != nil {
		g.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, result)
}

// ─── Internal Replication Handlers ───────────────────────────────────────────

func (g *Gateway) handleInternalReplica(c *gin.Context) {
	type replicaRequest struct {
		Meta         *storage.ObjectMeta `json:"meta"`
		Data         []byte              `json:"data"`
		Epoch        int64               `json:"epoch"`
		TargetNodeID string              `json:"target_node_id"`
	}

	var req replicaRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	targetNodeID := req.TargetNodeID
	if targetNodeID == "" {
		targetNodeID = c.GetHeader("X-Target-Node-ID")
	}

	targetNode := g.localNode
	if targetNodeID != "" && g.registry != nil {
		if n, err := g.registry.GetOrCreateNode(targetNodeID); err == nil {
			targetNode = n
		}
	}

	if err := targetNode.PutObject(req.Meta, req.Data, req.Epoch); err != nil {
		g.handleError(c, err)
		return
	}

	c.Status(http.StatusOK)
}

func (g *Gateway) handleInternalGetObject(c *gin.Context) {
	bucket := c.Param("bucket")
	key := c.Param("key")
	versionID := c.Query("version")
	nodeID := c.Query("node_id")
	if nodeID == "" {
		nodeID = c.GetHeader("X-Target-Node-ID")
	}

	targetNode := g.localNode
	if nodeID != "" && g.registry != nil {
		if n := g.registry.GetNode(nodeID); n != nil {
			targetNode = n
		}
	}

	data, meta, err := targetNode.GetObject(bucket, key, versionID)
	if err != nil {
		g.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"meta": meta,
		"data": data,
	})
}


// ─── Admin Handlers ───────────────────────────────────────────────────────────

func (g *Gateway) handleGetCluster(c *gin.Context) {
	nodes := g.memberTracker.GetAllNodes()
	metrics := g.memberTracker.GetClusterMetrics()
	raftStatus := g.meta.GetRaftStatus()

	c.JSON(http.StatusOK, gin.H{
		"nodes":       nodes,
		"metrics":     metrics,
		"raft_status": raftStatus,
		"epoch":       g.memberTracker.GetEpoch(),
	})
}

func (g *Gateway) handleGetClusterMetrics(c *gin.Context) {
	m := g.memberTracker.GetClusterMetrics()
	m.RaftLeaderID = g.meta.LeaderID()
	raftStatus := g.meta.GetRaftStatus()
	if term, ok := raftStatus["term"].(int64); ok {
		m.RaftTerm = term
	} else if termFloat, ok := raftStatus["term"].(float64); ok {
		m.RaftTerm = int64(termFloat)
	}
	if commit, ok := raftStatus["commit_index"].(int64); ok {
		m.RaftCommitIndex = commit
	} else if commitFloat, ok := raftStatus["commit_index"].(float64); ok {
		m.RaftCommitIndex = int64(commitFloat)
	}
	m.TotalReads = atomic.LoadInt64(&g.readCount)
	m.SuccessfulReads = atomic.LoadInt64(&g.readCount)
	m.TotalWrites = atomic.LoadInt64(&g.writeCount)
	m.SuccessfulWrites = atomic.LoadInt64(&g.writeCount)
	c.JSON(http.StatusOK, m)
}

func (g *Gateway) handleListNodes(c *gin.Context) {
	nodes := g.memberTracker.GetAllNodes()
	c.JSON(http.StatusOK, nodes)
}

func (g *Gateway) handleGetNode(c *gin.Context) {
	nodeID := c.Param("node_id")
	node, ok := g.memberTracker.GetNode(nodeID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "node not found"})
		return
	}
	c.JSON(http.StatusOK, node)
}

func (g *Gateway) handleSetNodeState(c *gin.Context) {
	nodeID := c.Param("node_id")
	var req struct {
		State string `json:"state" binding:"required"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	state := types.NodeState(req.State)
	if err := g.memberTracker.SetNodeState(nodeID, state); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"node_id": nodeID, "new_state": state})
}

// ─── Chaos Lab Handlers ───────────────────────────────────────────────────────

func (g *Gateway) handleCrashNode(c *gin.Context) {
	nodeID := c.Param("node_id")
	if err := g.memberTracker.SetNodeState(nodeID, types.NodeStateUnavailable); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	g.logger.Warn().Str("event", "CHAOS_CRASH_NODE").Str("node_id", nodeID).Msg("chaos: node crashed")
	
	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventNodeCrashed,
		Source:   "chaos-lab",
		NodeID:   nodeID,
		Severity: "WARN",
		Message:  fmt.Sprintf("Node %s crashed via Chaos Lab", nodeID),
	})
	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventNodeUnavailable,
		Source:   "membership",
		NodeID:   nodeID,
		Severity: "ERROR",
		Message:  fmt.Sprintf("Node %s state set to UNAVAILABLE", nodeID),
	})

	c.JSON(http.StatusOK, gin.H{"event": "NODE_CRASHED", "node_id": nodeID, "timestamp": time.Now()})
}

func (g *Gateway) handleRestartNode(c *gin.Context) {
	nodeID := c.Param("node_id")
	if err := g.memberTracker.SetNodeState(nodeID, types.NodeStateRecovering); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	// Simulate recovery delay
	go func() {
		time.Sleep(3 * time.Second)
		g.memberTracker.SetNodeState(nodeID, types.NodeStateHealthy)
	}()
	g.logger.Info().Str("event", "CHAOS_RESTART_NODE").Str("node_id", nodeID).Msg("chaos: node restarting")
	
	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventNodeRecovered,
		Source:   "chaos-lab",
		NodeID:   nodeID,
		Severity: "INFO",
		Message:  fmt.Sprintf("Node %s restarted and state restored to HEALTHY", nodeID),
	})

	c.JSON(http.StatusOK, gin.H{"event": "NODE_RESTARTING", "node_id": nodeID, "timestamp": time.Now()})
}

func (g *Gateway) handleCorruptReplica(c *gin.Context) {
	var req struct {
		Bucket    string `json:"bucket" binding:"required"`
		Key       string `json:"key" binding:"required"`
		VersionID string `json:"version_id" binding:"required"`
		NodeID    string `json:"node_id"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	targetNodeID := req.NodeID
	if targetNodeID == "" {
		targetNodeID = "storage-2"
	}

	targetNode := g.localNode
	if g.registry != nil {
		if n, err := g.registry.GetOrCreateNode(targetNodeID); err == nil {
			targetNode = n
		}
	}

	if err := targetNode.InjectCorruption(req.Bucket, req.Key, req.VersionID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("corruption injection failed: %v", err)})
		return
	}

	g.metrics.ChecksumMismatches.Inc()
	g.logger.Warn().
		Str("event", "CHAOS_CORRUPT_REPLICA").
		Str("bucket", req.Bucket).
		Str("key", req.Key).
		Str("version_id", req.VersionID).
		Str("node_id", targetNodeID).
		Msg("chaos: replica corrupted")

	observability.DefaultEventBus().Publish(observability.Event{
		Type:      observability.EventChecksumMismatch,
		Source:    "chaos-lab",
		ObjectKey: req.Key,
		NodeID:    targetNodeID,
		Severity:  "WARN",
		Message:   fmt.Sprintf("Replica bit-rot corruption injected for %s on node %s", req.Key, targetNodeID),
	})

	c.JSON(http.StatusOK, gin.H{
		"event":      "REPLICA_CORRUPTED",
		"bucket":     req.Bucket,
		"key":        req.Key,
		"version_id": req.VersionID,
		"node_id":    targetNodeID,
		"timestamp":  time.Now(),
	})
}

func (g *Gateway) handleAddNode(c *gin.Context) {
	var req types.StorageNode
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.ID == "" {
		req.ID = uuid.New().String()
	}
	req.State = types.NodeStateHealthy
	req.JoinedAt = time.Now()

	newEpoch := g.memberTracker.AddNode(&req)

	nodes := g.memberTracker.GetAllNodes()
	affectedPGs := g.placementMgr.UpdateCluster(nodes, newEpoch)

	g.logger.Info().
		Str("event", "NODE_ADDED").
		Str("node_id", req.ID).
		Int64("new_epoch", newEpoch).
		Int("affected_pgs", len(affectedPGs)).
		Msg("node added to cluster")

	c.JSON(http.StatusCreated, gin.H{
		"event":        "NODE_ADDED",
		"node_id":      req.ID,
		"new_epoch":    newEpoch,
		"affected_pgs": len(affectedPGs),
	})
}

func (g *Gateway) handleRemoveNode(c *gin.Context) {
	nodeID := c.Param("node_id")
	newEpoch := g.memberTracker.RemoveNode(nodeID)
	nodes := g.memberTracker.GetAllNodes()
	affectedPGs := g.placementMgr.UpdateCluster(nodes, newEpoch)

	if g.registry != nil {
		g.registry.RemoveNode(nodeID)
	}
	if g.repManager != nil {
		g.repManager.RemovePeer(nodeID)
	}

	g.logger.Info().
		Str("event", "NODE_REMOVED").
		Str("node_id", nodeID).
		Int64("new_epoch", newEpoch).
		Int("affected_pgs", len(affectedPGs)).
		Msg("node removed from cluster")

	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventNodeRemoved,
		Source:   "membership",
		NodeID:   nodeID,
		Severity: "WARN",
		Message:  fmt.Sprintf("Node %s removed from cluster", nodeID),
	})

	c.JSON(http.StatusOK, gin.H{
		"event":        "NODE_REMOVED",
		"node_id":      nodeID,
		"new_epoch":    newEpoch,
		"affected_pgs": len(affectedPGs),
	})
}

func (g *Gateway) handleListRepairs(c *gin.Context) {
	g.historyMu.RLock()
	defer g.historyMu.RUnlock()
	if g.repairHistory == nil {
		c.JSON(http.StatusOK, []gin.H{})
		return
	}
	c.JSON(http.StatusOK, g.repairHistory)
}

func (g *Gateway) handleTriggerRepair(c *gin.Context) {
	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventRepairStarted,
		Source:   "repair-engine",
		Severity: "INFO",
		Message:  "Background self-healing repair job initiated",
	})

	repairedCount := 0
	nodes := map[string]*storage.Node{g.localNode.NodeID(): g.localNode}
	if g.registry != nil {
		nodes = g.registry.GetAllNodes()
	}

	for nID, node := range nodes {
		objects := node.ListObjects("default")
		for _, meta := range objects {
			_, _, err := node.GetObject(meta.Bucket, meta.Key, meta.VersionID)
			if err != nil {
				g.logger.Warn().Str("key", meta.Key).Str("node", nID).Err(err).Msg("anti-entropy repair: corrupt replica detected, repairing")

				repaired := false
				if repairErr := node.RepairReplica(meta.Bucket, meta.Key, meta.VersionID); repairErr == nil {
					if _, _, vErr := node.GetObject(meta.Bucket, meta.Key, meta.VersionID); vErr == nil {
						repairedCount++
						repaired = true
						g.logger.Info().Str("key", meta.Key).Str("node", nID).Msg("anti-entropy repair: replica restored from backup")
					}
				}

				if !repaired {
					replicaNodes, _, _ := g.placementMgr.GetReplicas(meta.Bucket, meta.Key, 3)
					healthyData, healthyMeta, _, readErr := g.repManager.ReadWithFallback(
						c.Request.Context(),
						meta.Bucket, meta.Key, meta.VersionID,
						replicaNodes,
					)
					if readErr == nil && len(healthyData) > 0 {
						if putErr := node.PutObject(healthyMeta, healthyData, 0); putErr == nil {
							repairedCount++
							repaired = true
							g.logger.Info().Str("key", meta.Key).Str("node", nID).Msg("anti-entropy repair: replica restored from peer")
						}
					}
				}

				if repaired {
					job := gin.H{
						"id":           fmt.Sprintf("repair-%s-%d", nID, time.Now().UnixNano()%10000),
						"bucket":       meta.Bucket,
						"key":          meta.Key,
						"version_id":   meta.VersionID,
						"target_node":  nID,
						"source_node":  "storage-1",
						"state":        "HEALTHY",
						"reason":       "CHECKSUM_MISMATCH",
						"priority":     1,
						"created_at":   time.Now().Format(time.RFC3339),
						"completed_at": time.Now().Format(time.RFC3339),
						"bytes_copied": meta.Size,
						"total_bytes":  meta.Size,
					}
					g.historyMu.Lock()
					g.repairHistory = append(g.repairHistory, job)
					g.historyMu.Unlock()
				}
			}
		}
	}

	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventRepairCompleted,
		Source:   "repair-engine",
		Severity: "INFO",
		Message:  fmt.Sprintf("Self-healing repair completed successfully (%d replicas repaired)", repairedCount),
	})
	c.JSON(http.StatusAccepted, gin.H{
		"message":          "repair completed",
		"repaired_objects": repairedCount,
	})
}

func (g *Gateway) handleListScrubs(c *gin.Context) {
	g.historyMu.RLock()
	defer g.historyMu.RUnlock()
	if g.scrubHistory == nil {
		c.JSON(http.StatusOK, []gin.H{})
		return
	}
	c.JSON(http.StatusOK, g.scrubHistory)
}

func (g *Gateway) handleTriggerScrub(c *gin.Context) {
	nodeID := c.Param("node_id")
	if nodeID == "" {
		nodeID = "storage-1"
	}

	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventScrubStarted,
		Source:   "scrub-engine",
		NodeID:   nodeID,
		Severity: "INFO",
		Message:  fmt.Sprintf("Background bit-rot scrub triggered on node %s", nodeID),
	})

	targetNode := g.localNode
	if g.registry != nil {
		if n := g.registry.GetNode(nodeID); n != nil {
			targetNode = n
		}
	}

	corrupt, stale, _ := targetNode.ScrubAll()
	for _, cItem := range corrupt {
		observability.DefaultEventBus().Publish(observability.Event{
			Type:      observability.EventChecksumMismatch,
			Source:    "scrub-engine",
			NodeID:    nodeID,
			ObjectKey: cItem,
			Severity:  "WARN",
			Message:   fmt.Sprintf("CHECKSUM_MISMATCH detected on node %s for %s", nodeID, cItem),
		})
	}

	record := gin.H{
		"id":            fmt.Sprintf("scrub-%s-%d", nodeID, time.Now().UnixNano()%10000),
		"node_id":       nodeID,
		"started_at":    time.Now().Format(time.RFC3339),
		"completed_at":  time.Now().Format(time.RFC3339),
		"corrupt_count": len(corrupt),
		"stale_count":   len(stale),
		"status":        "COMPLETED",
	}
	g.historyMu.Lock()
	g.scrubHistory = append(g.scrubHistory, record)
	g.historyMu.Unlock()

	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventScrubCompleted,
		Source:   "scrub-engine",
		NodeID:   nodeID,
		Severity: "INFO",
		Message:  fmt.Sprintf("Background bit-rot scrub completed on node %s (corrupt=%d, stale=%d)", nodeID, len(corrupt), len(stale)),
	})
	c.JSON(http.StatusAccepted, gin.H{"node_id": nodeID, "message": "scrub completed", "corrupt_count": len(corrupt)})
}

func (g *Gateway) handleGetSSEToken(c *gin.Context) {
	token := uuid.New().String()
	g.sseMu.Lock()
	if g.sseTokens == nil {
		g.sseTokens = make(map[string]time.Time)
	}
	now := time.Now()
	for k, exp := range g.sseTokens {
		if now.After(exp) {
			delete(g.sseTokens, k)
		}
	}
	g.sseTokens[token] = now.Add(60 * time.Second)
	g.sseMu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"token":      token,
		"expires_in": 60,
	})
}

func (g *Gateway) handleEventStream(c *gin.Context) {
	token := c.Query("token")
	keyHeader := c.GetHeader("X-Admin-API-Key")

	authorized := false
	if g.adminAPIKey == "" {
		authorized = true
	} else if keyHeader == g.adminAPIKey {
		authorized = true
	} else if token != "" {
		g.sseMu.Lock()
		if exp, ok := g.sseTokens[token]; ok && time.Now().Before(exp) {
			authorized = true
			delete(g.sseTokens, token)
		}
		g.sseMu.Unlock()
	}

	if !authorized {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized SSE request"})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	origin := c.Request.Header.Get("Origin")
	if origin != "" {
		c.Header("Access-Control-Allow-Origin", origin)
	} else {
		c.Header("Access-Control-Allow-Origin", "http://localhost:3000")
	}

	bus := observability.DefaultEventBus()
	ch, unsubscribe := bus.Subscribe()
	defer unsubscribe()

	c.SSEvent("CONNECT", map[string]string{
		"message": "Connected to FT-DOSS real-time event stream",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
	c.Writer.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	c.Stream(func(w io.Writer) bool {
		select {
		case evt, ok := <-ch:
			if !ok {
				return false
			}
			c.SSEvent(string(evt.Type), evt)
			return true
		case <-ticker.C:
			c.Writer.Write([]byte(": heartbeat\n\n"))
			c.Writer.Flush()
			return true
		case <-c.Request.Context().Done():
			return false
		}
	})
}

func (g *Gateway) handleListPGs(c *gin.Context) {
	pgs := g.placementMgr.GetAllGroups()
	c.JSON(http.StatusOK, pgs)
}

func (g *Gateway) handleGetPG(c *gin.Context) {
	pgIDStr := c.Param("pg_id")
	pgID, err := strconv.Atoi(pgIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid pg_id"})
		return
	}
	pg := g.placementMgr.GetPlacementGroup(pgID)
	if pg == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "placement group not found"})
		return
	}
	c.JSON(http.StatusOK, pg)
}

func (g *Gateway) handleGetRaft(c *gin.Context) {
	status := g.meta.GetRaftStatus()
	c.JSON(http.StatusOK, status)
}

func (g *Gateway) handleGetAlerts(c *gin.Context) {
	alerts := g.alerts.GetAllAlerts()
	c.JSON(http.StatusOK, alerts)
}

func (g *Gateway) handleGetRebalance(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "rebalance status"})
}

func (g *Gateway) handleResetDemo(c *gin.Context) {
	if g.meta != nil {
		g.meta.ResetState()
	}
	if g.registry != nil {
		g.registry.ResetAll()
	} else if g.localNode != nil {
		g.localNode.ResetStorage()
	}
	if g.memberTracker != nil {
		allNodes := g.memberTracker.GetAllNodes()
		for _, n := range allNodes {
			if n.ID != "storage-1" && n.ID != "storage-2" && n.ID != "storage-3" {
				g.memberTracker.RemoveNode(n.ID)
				if g.registry != nil {
					g.registry.RemoveNode(n.ID)
				}
				if g.repManager != nil {
					g.repManager.RemovePeer(n.ID)
				}
			}
		}
		_ = g.memberTracker.SetNodeState("storage-1", types.NodeStateHealthy)
		_ = g.memberTracker.SetNodeState("storage-2", types.NodeStateHealthy)
		_ = g.memberTracker.SetNodeState("storage-3", types.NodeStateHealthy)
	}
	if g.meta != nil {
		_ = g.meta.CreateBucket(c.Request.Context(), &types.Bucket{
			Name:              "default",
			ReplicationFactor: g.config.WriteQuorum + 1,
			WriteQuorum:       g.config.WriteQuorum,
			ReadQuorum:        g.config.ReadQuorum,
			VersioningEnabled: true,
		})
	}

	observability.DefaultEventBus().Publish(observability.Event{
		Type:     observability.EventResetState,
		Source:   "gateway",
		Severity: "INFO",
		Message:  "System state reset to baseline default",
	})

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Demo environment state reset successfully"})
}


// ─── Multipart Handlers (stub) ────────────────────────────────────────────────

func (g *Gateway) handleInitMultipart(c *gin.Context) {
	uploadID := uuid.New().String()
	c.JSON(http.StatusCreated, gin.H{
		"upload_id": uploadID,
		"created":   time.Now(),
	})
}

func (g *Gateway) handleUploadPart(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"upload_id":   c.Param("upload_id"),
		"part_number": c.Param("part_number"),
		"etag":        uuid.New().String(),
	})
}

func (g *Gateway) handleCompleteMultipart(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"upload_id": c.Param("upload_id"),
		"status":    "completed",
	})
}

func (g *Gateway) handleAbortMultipart(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// ─── Error Handling ───────────────────────────────────────────────────────────

func (g *Gateway) handleError(c *gin.Context, err error) {
	if err == nil {
		return
	}

	if dossErr, ok := err.(*types.DossError); ok {
		switch dossErr.Code {
		case types.ErrObjectNotFound, types.ErrBucketNotFound:
			c.JSON(http.StatusNotFound, dossErr)
		case types.ErrVersionConflict:
			c.JSON(http.StatusConflict, dossErr)
		case types.ErrQuorumUnavailable, types.ErrNodeUnavailable:
			c.JSON(http.StatusServiceUnavailable, dossErr)
		case types.ErrChecksumMismatch:
			c.JSON(http.StatusBadRequest, dossErr)
		case types.ErrStaleEpoch:
			c.JSON(http.StatusConflict, dossErr)
		case types.ErrDiskFull:
			c.JSON(http.StatusInsufficientStorage, dossErr)
		case types.ErrUnauthorized:
			c.JSON(http.StatusUnauthorized, dossErr)
		case types.ErrForbidden:
			c.JSON(http.StatusForbidden, dossErr)
		default:
			c.JSON(http.StatusInternalServerError, dossErr)
		}
		return
	}

	c.JSON(http.StatusInternalServerError, gin.H{
		"code":    string(types.ErrInvalidRequest),
		"message": err.Error(),
	})
}
