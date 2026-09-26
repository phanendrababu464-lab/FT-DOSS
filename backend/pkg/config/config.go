// Package config provides centralized configuration for FT-DOSS.
package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config is the top-level configuration structure for FT-DOSS.
type Config struct {
	Node     NodeConfig     `mapstructure:"node"`
	Cluster  ClusterConfig  `mapstructure:"cluster"`
	Storage  StorageConfig  `mapstructure:"storage"`
	Metadata MetadataConfig `mapstructure:"metadata"`
	Gateway  GatewayConfig  `mapstructure:"gateway"`
	Replication ReplicationConfig `mapstructure:"replication"`
	Repair   RepairConfig   `mapstructure:"repair"`
	Scrub    ScrubConfig    `mapstructure:"scrub"`
	Rebalance RebalanceConfig `mapstructure:"rebalance"`
	Security SecurityConfig `mapstructure:"security"`
	Observability ObservabilityConfig `mapstructure:"observability"`
}

// NodeConfig defines identity and location of this node.
type NodeConfig struct {
	ID       string `mapstructure:"id"`
	Role     string `mapstructure:"role"` // gateway, metadata, storage, repair
	Address  string `mapstructure:"address"`
	HTTPPort int    `mapstructure:"http_port"`
	GRPCPort int    `mapstructure:"grpc_port"`
	Zone     string `mapstructure:"zone"`
	Rack     string `mapstructure:"rack"`
	Region   string `mapstructure:"region"`
}

// ClusterConfig defines cluster-wide settings.
type ClusterConfig struct {
	PlacementGroupCount    int           `mapstructure:"placement_group_count"`
	HeartbeatInterval      time.Duration `mapstructure:"heartbeat_interval"`
	FailureSuspectTimeout  time.Duration `mapstructure:"failure_suspect_timeout"`
	FailureUnavailTimeout  time.Duration `mapstructure:"failure_unavail_timeout"`
	FencingEnabled         bool          `mapstructure:"fencing_enabled"`
	MinHealthyNodes        int           `mapstructure:"min_healthy_nodes"`
	MetadataNodes          []string      `mapstructure:"metadata_nodes"`
	StorageNodes           []string      `mapstructure:"storage_nodes"`
}

// StorageConfig configures local storage node behavior.
type StorageConfig struct {
	DataDir         string  `mapstructure:"data_dir"`
	WalDir          string  `mapstructure:"wal_dir"`
	MaxObjectSizeMB int64   `mapstructure:"max_object_size_mb"`
	DiskFullThreshold float64 `mapstructure:"disk_full_threshold"` // 0.0-1.0
	ChunkSizeMB     int64   `mapstructure:"chunk_size_mb"`
}

// MetadataConfig configures the metadata/Raft service.
type MetadataConfig struct {
	DatabaseURL      string        `mapstructure:"database_url"`
	RaftPeers        []string      `mapstructure:"raft_peers"`
	RaftPort         int           `mapstructure:"raft_port"`
	ElectionTimeout  time.Duration `mapstructure:"election_timeout"`
	HeartbeatTimeout time.Duration `mapstructure:"heartbeat_timeout"`
	SnapshotInterval int64         `mapstructure:"snapshot_interval"`
}

// GatewayConfig configures the API gateway.
type GatewayConfig struct {
	Port          int           `mapstructure:"port"`
	TLSEnabled    bool          `mapstructure:"tls_enabled"`
	TLSCertFile   string        `mapstructure:"tls_cert_file"`
	TLSKeyFile    string        `mapstructure:"tls_key_file"`
	MaxBodySizeMB int64         `mapstructure:"max_body_size_mb"`
	ReadTimeout   time.Duration `mapstructure:"read_timeout"`
	WriteTimeout  time.Duration `mapstructure:"write_timeout"`
	RateLimit     int           `mapstructure:"rate_limit"` // req/s
	MetadataAddr  string        `mapstructure:"metadata_addr"`
}

// ReplicationConfig configures replication policy defaults.
type ReplicationConfig struct {
	Factor      int    `mapstructure:"factor"`
	WriteQuorum int    `mapstructure:"write_quorum"`
	ReadQuorum  int    `mapstructure:"read_quorum"`
	Algorithm   string `mapstructure:"checksum_algorithm"`
	FailureDomain string `mapstructure:"failure_domain"` // rack, zone, region
}

// RepairConfig configures the background repair scheduler.
type RepairConfig struct {
	Workers          int           `mapstructure:"workers"`
	ScanInterval     time.Duration `mapstructure:"scan_interval"`
	MaxBandwidthMBPS int           `mapstructure:"max_bandwidth_mbps"`
	Enabled          bool          `mapstructure:"enabled"`
}

// ScrubConfig configures the background scrubber.
type ScrubConfig struct {
	LightInterval  time.Duration `mapstructure:"light_interval"`
	DeepInterval   time.Duration `mapstructure:"deep_interval"`
	Concurrency    int           `mapstructure:"concurrency"`
	MaxBandwidthMBPS int         `mapstructure:"max_bandwidth_mbps"`
	Enabled        bool          `mapstructure:"enabled"`
}

// RebalanceConfig configures the rebalancing system.
type RebalanceConfig struct {
	MaxBandwidthMBPS int           `mapstructure:"max_bandwidth_mbps"`
	Enabled          bool          `mapstructure:"enabled"`
	ScanInterval     time.Duration `mapstructure:"scan_interval"`
}

// SecurityConfig configures security settings.
type SecurityConfig struct {
	JWTSecret      string        `mapstructure:"jwt_secret"`
	TokenExpiry    time.Duration `mapstructure:"token_expiry"`
	AdminAPIKey    string        `mapstructure:"admin_api_key"`
	EnableAuditLog bool          `mapstructure:"enable_audit_log"`
}

// ObservabilityConfig configures metrics and logging.
type ObservabilityConfig struct {
	PrometheusPort  int    `mapstructure:"prometheus_port"`
	LogLevel        string `mapstructure:"log_level"`
	LogFormat       string `mapstructure:"log_format"` // json, text
	TracingEnabled  bool   `mapstructure:"tracing_enabled"`
}

// DefaultConfig returns a sensible default configuration for development.
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:       "node-1",
			Role:     "storage",
			Address:  "localhost",
			HTTPPort: 8080,
			GRPCPort: 9090,
			Zone:     "zone-1",
			Rack:     "rack-1",
			Region:   "us-east-1",
		},
		Cluster: ClusterConfig{
			PlacementGroupCount:   1024,
			HeartbeatInterval:     5 * time.Second,
			FailureSuspectTimeout: 15 * time.Second,
			FailureUnavailTimeout: 30 * time.Second,
			FencingEnabled:        true,
			MinHealthyNodes:       2,
		},
		Storage: StorageConfig{
			DataDir:           "./data",
			WalDir:            "./wal",
			MaxObjectSizeMB:   10240,
			DiskFullThreshold: 0.90,
			ChunkSizeMB:       64,
		},
		Metadata: MetadataConfig{
			ElectionTimeout:  150 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
			SnapshotInterval: 1000,
		},
		Gateway: GatewayConfig{
			Port:          8000,
			MaxBodySizeMB: 10240,
			ReadTimeout:   30 * time.Second,
			WriteTimeout:  30 * time.Second,
			RateLimit:     1000,
		},
		Replication: ReplicationConfig{
			Factor:        3,
			WriteQuorum:   2,
			ReadQuorum:    2,
			Algorithm:     "sha256",
			FailureDomain: "rack",
		},
		Repair: RepairConfig{
			Workers:          4,
			ScanInterval:     30 * time.Second,
			MaxBandwidthMBPS: 100,
			Enabled:          true,
		},
		Scrub: ScrubConfig{
			LightInterval:    1 * time.Hour,
			DeepInterval:     24 * time.Hour,
			Concurrency:      2,
			MaxBandwidthMBPS: 50,
			Enabled:          true,
		},
		Rebalance: RebalanceConfig{
			MaxBandwidthMBPS: 50,
			Enabled:          true,
			ScanInterval:     1 * time.Minute,
		},
		Security: SecurityConfig{
			JWTSecret:      "change-me-in-production",
			TokenExpiry:    24 * time.Hour,
			EnableAuditLog: true,
		},
		Observability: ObservabilityConfig{
			PrometheusPort: 2112,
			LogLevel:       "info",
			LogFormat:      "json",
		},
	}
}

// Load reads configuration from file and environment variables.
func Load(configFile string) (*Config, error) {
	v := viper.New()

	// Set defaults
	def := DefaultConfig()
	v.SetDefault("node.id", def.Node.ID)
	v.SetDefault("node.http_port", def.Node.HTTPPort)
	v.SetDefault("node.grpc_port", def.Node.GRPCPort)
	v.SetDefault("cluster.placement_group_count", def.Cluster.PlacementGroupCount)
	v.SetDefault("cluster.heartbeat_interval", def.Cluster.HeartbeatInterval)
	v.SetDefault("replication.factor", def.Replication.Factor)
	v.SetDefault("replication.write_quorum", def.Replication.WriteQuorum)
	v.SetDefault("replication.read_quorum", def.Replication.ReadQuorum)
	v.SetDefault("replication.checksum_algorithm", def.Replication.Algorithm)

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
	}

	v.AutomaticEnv()
	v.SetEnvPrefix("FTDOSS")

	cfg := DefaultConfig()
	_ = v.Unmarshal(cfg)

	// Apply defaults for unset fields
	if cfg.Cluster.PlacementGroupCount == 0 {
		cfg.Cluster.PlacementGroupCount = def.Cluster.PlacementGroupCount
	}
	if cfg.Replication.Factor == 0 {
		cfg.Replication.Factor = def.Replication.Factor
	}
	if cfg.Replication.WriteQuorum == 0 {
		cfg.Replication.WriteQuorum = def.Replication.WriteQuorum
	}
	if cfg.Replication.ReadQuorum == 0 {
		cfg.Replication.ReadQuorum = def.Replication.ReadQuorum
	}
	if cfg.Node.ID == "" {
		cfg.Node.ID = def.Node.ID
	}
	if cfg.Node.HTTPPort == 0 {
		cfg.Node.HTTPPort = def.Node.HTTPPort
	}
	if cfg.Storage.DataDir == "" {
		cfg.Storage.DataDir = def.Storage.DataDir
	}
	if cfg.Storage.WalDir == "" {
		cfg.Storage.WalDir = def.Storage.WalDir
	}

	return cfg, nil
}
