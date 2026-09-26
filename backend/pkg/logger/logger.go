// Package logger provides structured JSON logging for FT-DOSS.
package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Fields are standard log fields used throughout the system.
type Fields struct {
	RequestID      string
	OperationID    string
	NodeID         string
	ObjectID       string
	VersionID      string
	PlacementGroup int
	ClusterEpoch   int64
	Bucket         string
	Key            string
	Event          string
	Status         string
	LatencyMS      float64
}

// Logger wraps zerolog.Logger with distributed-system context.
type Logger struct {
	zl zerolog.Logger
}

// New creates a new structured logger.
func New(level, format, nodeID string) *Logger {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	var zl zerolog.Logger
	if format == "text" {
		zl = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
			Level(lvl).
			With().
			Timestamp().
			Str("node_id", nodeID).
			Logger()
	} else {
		zl = zerolog.New(os.Stdout).
			Level(lvl).
			With().
			Timestamp().
			Str("node_id", nodeID).
			Logger()
	}

	return &Logger{zl: zl}
}

// WithFields returns a logger with additional context fields.
func (l *Logger) WithFields(f Fields) *zerolog.Logger {
	ctx := l.zl.With()

	if f.RequestID != "" {
		ctx = ctx.Str("request_id", f.RequestID)
	}
	if f.OperationID != "" {
		ctx = ctx.Str("operation_id", f.OperationID)
	}
	if f.ObjectID != "" {
		ctx = ctx.Str("object_id", f.ObjectID)
	}
	if f.VersionID != "" {
		ctx = ctx.Str("version_id", f.VersionID)
	}
	if f.PlacementGroup > 0 {
		ctx = ctx.Int("placement_group", f.PlacementGroup)
	}
	if f.ClusterEpoch > 0 {
		ctx = ctx.Int64("cluster_epoch", f.ClusterEpoch)
	}
	if f.Bucket != "" {
		ctx = ctx.Str("bucket", f.Bucket)
	}
	if f.Key != "" {
		ctx = ctx.Str("key", f.Key)
	}
	if f.Event != "" {
		ctx = ctx.Str("event", f.Event)
	}
	if f.LatencyMS > 0 {
		ctx = ctx.Float64("latency_ms", f.LatencyMS)
	}

	zl := ctx.Logger()
	return &zl
}

// Info logs an informational event.
func (l *Logger) Info() *zerolog.Event { return l.zl.Info() }

// Warn logs a warning event.
func (l *Logger) Warn() *zerolog.Event { return l.zl.Warn() }

// Error logs an error event.
func (l *Logger) Error() *zerolog.Event { return l.zl.Error() }

// Debug logs a debug event.
func (l *Logger) Debug() *zerolog.Event { return l.zl.Debug() }

// Fatal logs a fatal event and exits.
func (l *Logger) Fatal() *zerolog.Event { return l.zl.Fatal() }

// With returns a child logger context builder.
func (l *Logger) With() zerolog.Context { return l.zl.With() }

// ReplicaRepairCompleted logs a replica repair completion event.
func (l *Logger) ReplicaRepairCompleted(objectID, versionID, nodeID, checksum string, durationMS float64) {
	l.zl.Info().
		Str("event", "REPLICA_REPAIR_COMPLETED").
		Str("object_id", objectID).
		Str("version_id", versionID).
		Str("node_id", nodeID).
		Str("checksum", checksum).
		Float64("duration_ms", durationMS).
		Msg("replica repaired")
}

// WritePath logs write path state transitions.
func (l *Logger) WritePath(requestID, operationID, bucket, key, state string) {
	l.zl.Info().
		Str("event", "WRITE_PATH").
		Str("request_id", requestID).
		Str("operation_id", operationID).
		Str("bucket", bucket).
		Str("key", key).
		Str("state", state).
		Msg("write path state transition")
}

// ReadPath logs read path events.
func (l *Logger) ReadPath(requestID, bucket, key, versionID, nodeID string) {
	l.zl.Info().
		Str("event", "READ_PATH").
		Str("request_id", requestID).
		Str("bucket", bucket).
		Str("key", key).
		Str("version_id", versionID).
		Str("source_node", nodeID).
		Msg("object read")
}

// ChecksumMismatch logs corruption detection events.
func (l *Logger) ChecksumMismatch(objectID, versionID, nodeID, expected, got string) {
	l.zl.Warn().
		Str("event", "CHECKSUM_MISMATCH").
		Str("object_id", objectID).
		Str("version_id", versionID).
		Str("node_id", nodeID).
		Str("expected_checksum", expected).
		Str("actual_checksum", got).
		Msg("checksum mismatch detected - possible corruption")
}

// NodeStateChange logs node health state transitions.
func (l *Logger) NodeStateChange(nodeID, fromState, toState string, epoch int64) {
	l.zl.Info().
		Str("event", "NODE_STATE_CHANGE").
		Str("node_id", nodeID).
		Str("from_state", fromState).
		Str("to_state", toState).
		Int64("cluster_epoch", epoch).
		Msg("node state changed")
}

// RaftEvent logs Raft consensus events.
func (l *Logger) RaftEvent(eventType, nodeID string, term, index int64) {
	l.zl.Info().
		Str("event", "RAFT_"+eventType).
		Str("node_id", nodeID).
		Int64("term", term).
		Int64("index", index).
		Msg("raft event")
}
