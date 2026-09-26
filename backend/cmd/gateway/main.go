// cmd/gateway/main.go - FT-DOSS API Gateway entry point.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ft-doss/backend/internal/gateway"
	"github.com/ft-doss/backend/internal/membership"
	"github.com/ft-doss/backend/internal/metadata"
	"github.com/ft-doss/backend/internal/observability"
	"github.com/ft-doss/backend/internal/placement"
	"github.com/ft-doss/backend/internal/raft"
	"github.com/ft-doss/backend/internal/replication"
	"github.com/ft-doss/backend/internal/storage"
	"github.com/ft-doss/backend/pkg/config"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

func main() {
	// Load configuration
	cfgFile := os.Getenv("FTDOSS_CONFIG_FILE")
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}

	// Override from environment
	if v := os.Getenv("FTDOSS_NODE_ID"); v != "" {
		cfg.Node.ID = v
	}
	if v := os.Getenv("FTDOSS_DATA_DIR"); v != "" {
		cfg.Storage.DataDir = v
	}
	if v := os.Getenv("FTDOSS_WAL_DIR"); v != "" {
		cfg.Storage.WalDir = v
	}
	if v := os.Getenv("FTDOSS_ZONE"); v != "" {
		cfg.Node.Zone = v
	}
	if v := os.Getenv("FTDOSS_RACK"); v != "" {
		cfg.Node.Rack = v
	}
	if v := os.Getenv("FTDOSS_ADMIN_API_KEY"); v != "" {
		cfg.Security.AdminAPIKey = v
	}

	// Logger
	log := logger.New(cfg.Observability.LogLevel, cfg.Observability.LogFormat, cfg.Node.ID)
	log.Info().Str("node_id", cfg.Node.ID).Str("version", "1.0.0").Msg("FT-DOSS starting")

	// Metrics
	metrics := observability.New(cfg.Node.ID)
	alerts := observability.NewAlertManager()

	// Membership tracker
	tracker := membership.NewTracker(
		cfg.Cluster.FailureSuspectTimeout,
		cfg.Cluster.FailureUnavailTimeout,
		log,
	)
	tracker.Start()

	// Storage node registry
	registry := storage.NewRegistry(cfg.Storage.DataDir, cfg.Storage.WalDir, cfg.Cluster.FencingEnabled, log)

	// Register 3 default storage nodes in registry and membership tracker
	nodeIDs := []string{"storage-1", "storage-2", "storage-3"}
	for _, nID := range nodeIDs {
		_, _ = registry.GetOrCreateNode(nID)
		tracker.RegisterNode(&types.StorageNode{
			ID:        nID,
			Address:   "localhost",
			HTTPPort:  8080,
			GRPCPort:  9090,
			Zone:      "zone-1",
			Rack:      "rack-1",
			State:     types.NodeStateHealthy,
			JoinedAt:  time.Now(),
			DiskTotal: 100 * 1024 * 1024 * 1024,
			DiskFree:  90 * 1024 * 1024 * 1024,
		})
	}

	localNode, err := registry.GetOrCreateNode(cfg.Node.ID)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create local storage node")
	}

	// Placement manager
	pgMgr := placement.NewManager(
		cfg.Cluster.PlacementGroupCount,
		cfg.Replication.FailureDomain,
	)

	// Raft node (single-node mode for MVP)
	// For full deployment, pass peer addresses from config
	inMemTransport := &inMemTransport{}

	// Create metadata service first (will be used as the Raft state machine)
	// We need a forward reference: create the adapter after metaSvc is created
	metaSvc := metadata.NewService(cfg.Node.ID, nil, nil, log) // raftNode set below

	// Create raft node with the metadata service as the state machine
	raftAdapter := &raftStateMachineAdapter{sm: metaSvc}
	raftNode := raft.NewNode(cfg.Node.ID, []string{}, raftAdapter, inMemTransport, log)
	raftNode.Start()

	// Wire the raft node back into the metadata service
	metaSvc.SetRaftNode(raftNode)

	// Create a default bucket on startup
	time.Sleep(200 * time.Millisecond) // wait for Raft leader election
	startupCtx := context.Background()
	metaSvc.CreateBucket(startupCtx, &types.Bucket{ //nolint
		Name:              "default",
		ReplicationFactor: cfg.Replication.Factor,
		WriteQuorum:       cfg.Replication.WriteQuorum,
		ReadQuorum:        cfg.Replication.ReadQuorum,
		VersioningEnabled: true,
	})

	// Register any peers from environment
	// PEER_NODES="node-2=http://storage-2:8080,node-3=http://storage-3:8080"
	if peers := os.Getenv("FTDOSS_PEER_NODES"); peers != "" {
		parsePeers(peers, nil, tracker, pgMgr) // tracker and placement only for now
	}

	// Initial cluster update
	pgMgr.UpdateCluster(tracker.GetAllNodes(), tracker.GetEpoch())

	writeQuorum := cfg.Replication.WriteQuorum
	readQuorum := cfg.Replication.ReadQuorum

	// Replication manager
	repMgr := replication.NewManager(localNode, writeQuorum, readQuorum, log)
	repMgr.SetRegistry(registry)

	// Register peers with replication manager
	if peers := os.Getenv("FTDOSS_PEER_NODES"); peers != "" {
		parsePeers(peers, repMgr, nil, nil)
	}

	// Gateway
	gw := gateway.NewGateway(
		metaSvc,
		pgMgr,
		repMgr,
		tracker,
		localNode,
		metrics,
		alerts,
		&gateway.GatewayConfig{
			WriteQuorum: writeQuorum,
			ReadQuorum:  readQuorum,
			MaxBodySize: cfg.Storage.MaxObjectSizeMB * 1024 * 1024,
			AdminAPIKey: cfg.Security.AdminAPIKey,
		},
		log,
	)
	gw.SetRegistry(registry)


	// Start Prometheus metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", observability.Handler())
		addr := fmt.Sprintf(":%d", cfg.Observability.PrometheusPort)
		log.Info().Str("addr", addr).Msg("starting Prometheus metrics server")
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Error().Err(err).Msg("metrics server error")
		}
	}()

	// Start background heartbeat sender for all registered storage nodes
	go func() {
		ticker := time.NewTicker(cfg.Cluster.HeartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			currEpoch := tracker.GetEpoch()
			for _, node := range registry.GetAllNodes() {
				if nState, ok := tracker.GetNode(node.NodeID()); ok && (nState.State == types.NodeStateUnavailable || nState.State == types.NodeStateDecommissioning) {
					continue
				}
				hb := node.Heartbeat()
				hb.ClusterEpoch = currEpoch
				_ = tracker.ProcessHeartbeat(hb)
			}
		}
	}()

	// Start alert rule evaluation
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			m := tracker.GetClusterMetrics()
			alerts.CheckRules(m)
		}
	}()

	// Start HTTP server
	port := cfg.Node.HTTPPort
	if port == 0 {
		port = 8080
	}
	if p := os.Getenv("FTDOSS_HTTP_PORT"); p != "" {
		fmt.Sscanf(p, "%d", &port)
	}

	addr := fmt.Sprintf(":%d", port)
	log.Info().
		Str("addr", addr).
		Str("node_id", cfg.Node.ID).
		Msg("FT-DOSS gateway listening")

	srv := &http.Server{
		Addr:         addr,
		Handler:      gw.Router(),
		ReadTimeout:  cfg.Gateway.ReadTimeout,
		WriteTimeout: cfg.Gateway.WriteTimeout,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal().Err(err).Msg("server failed")
	}
}

// ─── In-Memory Raft Transport (single-node mode) ─────────────────────────────

type inMemTransport struct{}

func (t *inMemTransport) AppendEntries(ctx context.Context, peerID string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return nil, fmt.Errorf("no peers in single-node mode")
}

func (t *inMemTransport) RequestVote(ctx context.Context, peerID string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return nil, fmt.Errorf("no peers in single-node mode")
}

// ─── Raft State Machine Adapter ──────────────────────────────────────────────

type raftStateMachineAdapter struct {
	sm *metadata.Service
}

func (a *raftStateMachineAdapter) Apply(cmd raft.LogCommand) error {
	return a.sm.Apply(cmd)
}

func (a *raftStateMachineAdapter) Snapshot() ([]byte, error) {
	return a.sm.Snapshot()
}

func (a *raftStateMachineAdapter) Restore(data []byte) error {
	return a.sm.Restore(data)
}

// ─── Peer Parsing ────────────────────────────────────────────────────────────

func parsePeers(peersEnv string, repMgr *replication.Manager, tracker *membership.Tracker, pgMgr *placement.Manager) {
	// Format: "node-2=http://host:8080,node-3=http://host:8080"
	var i, j int
	for i = 0; i <= len(peersEnv); i++ {
		if i == len(peersEnv) || peersEnv[i] == ',' {
			pair := peersEnv[j:i]
			j = i + 1
			// Split on '='
			for k := 0; k < len(pair); k++ {
				if pair[k] == '=' {
					nodeID := pair[:k]
					url := pair[k+1:]
					if repMgr != nil {
						repMgr.AddPeer(nodeID, url)
					}
					if tracker != nil {
						tracker.RegisterNode(&types.StorageNode{
							ID:    nodeID,
							State: types.NodeStateHealthy,
						})
					}
					break
				}
			}
		}
	}
}
