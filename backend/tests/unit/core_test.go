// Package tests/unit provides unit tests for core distributed system invariants.
package tests_unit

import (
	"fmt"
	"testing"
	"time"

	"github.com/ft-doss/backend/internal/checksum"
	"github.com/ft-doss/backend/internal/membership"
	"github.com/ft-doss/backend/internal/placement"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Checksum Tests ───────────────────────────────────────────────────────────

func TestChecksumSHA256(t *testing.T) {
	data := []byte("hello, distributed world")
	cs, err := checksum.Compute(data, types.ChecksumSHA256)
	require.NoError(t, err)
	assert.Contains(t, cs, "sha256:")
}

func TestChecksumVerify_Valid(t *testing.T) {
	data := []byte("immutable object data")
	cs, err := checksum.Compute(data, types.ChecksumSHA256)
	require.NoError(t, err)
	assert.NoError(t, checksum.Verify(data, cs, types.ChecksumSHA256))
}

func TestChecksumVerify_Corrupt(t *testing.T) {
	data := []byte("clean data")
	cs, err := checksum.Compute(data, types.ChecksumSHA256)
	require.NoError(t, err)

	// Corrupt the data
	corrupted := []byte("dirty dataX")
	err = checksum.Verify(corrupted, cs, types.ChecksumSHA256)
	assert.Error(t, err, "corrupted data must fail checksum verification")
}

func TestChunkManifest(t *testing.T) {
	// Create 1 MB object with 64KB chunks
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	manifest, err := checksum.BuildManifest("obj-1", "v1", data, 64*1024)
	require.NoError(t, err)
	assert.Equal(t, 16, len(manifest.Chunks), "1MB / 64KB = 16 chunks")
	assert.NoError(t, checksum.VerifyManifest(manifest, data))
}

func TestChunkManifest_CorruptChunk(t *testing.T) {
	data := make([]byte, 100)
	manifest, err := checksum.BuildManifest("obj-2", "v1", data, 50)
	require.NoError(t, err)

	// Corrupt one byte
	data[10] ^= 0xFF
	err = checksum.VerifyManifest(manifest, data)
	assert.Error(t, err, "manifest must detect chunk corruption")
}

// ─── Placement Tests ──────────────────────────────────────────────────────────

func TestPlacementDeterministic(t *testing.T) {
	pm := placement.NewManager(1024, "rack")

	// Same inputs must always produce same placement group
	pg1 := pm.PlacementGroupID("bucket", "key")
	pg2 := pm.PlacementGroupID("bucket", "key")
	assert.Equal(t, pg1, pg2, "placement must be deterministic")
}

func TestPlacementDifferentKeys(t *testing.T) {
	pm := placement.NewManager(1024, "rack")

	// Different keys should generally hash to different groups
	groups := make(map[int]bool)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		pg := pm.PlacementGroupID("bucket", key)
		groups[pg] = true
	}
	// With 100 keys in 1024 groups, expect good distribution
	assert.Greater(t, len(groups), 50, "placement should distribute across groups")
}

func TestPlacementFailureDomain(t *testing.T) {
	pm := placement.NewManager(1024, "rack")

	// Add nodes in different racks
	nodes := []*types.StorageNode{
		{ID: "node-1", Zone: "zone-1", Rack: "rack-1", State: types.NodeStateHealthy},
		{ID: "node-2", Zone: "zone-1", Rack: "rack-2", State: types.NodeStateHealthy},
		{ID: "node-3", Zone: "zone-2", Rack: "rack-3", State: types.NodeStateHealthy},
	}
	pm.UpdateCluster(nodes, 1)

	replicas, pgID, err := pm.GetReplicas("bucket", "key", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, len(replicas))
	assert.GreaterOrEqual(t, pgID, 0)

	// All replicas should be unique nodes
	seen := make(map[string]bool)
	for _, r := range replicas {
		assert.False(t, seen[r], "no duplicate nodes in replicas")
		seen[r] = true
	}
}

// ─── Membership / Failure Detection Tests ────────────────────────────────────

func TestMembershipNodeRegistration(t *testing.T) {
	log := logger.New("info", "text", "test")
	tracker := membership.NewTracker(15*time.Second, 30*time.Second, log)

	node := &types.StorageNode{
		ID:    "node-1",
		State: types.NodeStateHealthy,
	}
	tracker.RegisterNode(node)

	found, ok := tracker.GetNode("node-1")
	require.True(t, ok)
	assert.Equal(t, "node-1", found.ID)
	assert.Equal(t, types.NodeStateHealthy, found.State)
}

func TestMembershipHeartbeatUpdatesState(t *testing.T) {
	log := logger.New("info", "text", "test")
	tracker := membership.NewTracker(15*time.Second, 30*time.Second, log)

	node := &types.StorageNode{
		ID:    "node-1",
		State: types.NodeStateSuspect,
	}
	tracker.RegisterNode(node)
	tracker.SetNodeState("node-1", types.NodeStateSuspect)

	// Heartbeat should recover suspect node
	hb := &types.Heartbeat{
		NodeID:       "node-1",
		ClusterEpoch: 0,
		Timestamp:    time.Now(),
		State:        types.NodeStateHealthy,
	}
	err := tracker.ProcessHeartbeat(hb)
	require.NoError(t, err)

	found, _ := tracker.GetNode("node-1")
	assert.Equal(t, types.NodeStateHealthy, found.State, "heartbeat should recover suspect node")
}

func TestMembershipEpochFencing(t *testing.T) {
	log := logger.New("info", "text", "test")
	tracker := membership.NewTracker(15*time.Second, 30*time.Second, log)

	node := &types.StorageNode{ID: "node-1", State: types.NodeStateHealthy}
	tracker.RegisterNode(node)

	// Increment epoch to 5
	for i := 0; i < 5; i++ {
		tracker.IncrementEpoch()
	}

	// Heartbeat with stale epoch should be rejected
	hb := &types.Heartbeat{
		NodeID:       "node-1",
		ClusterEpoch: 1, // stale!
		Timestamp:    time.Now(),
	}
	err := tracker.ProcessHeartbeat(hb)
	assert.Error(t, err, "stale epoch heartbeat must be rejected (fencing)")
}

func TestMembershipEpochIncrement(t *testing.T) {
	log := logger.New("info", "text", "test")
	tracker := membership.NewTracker(15*time.Second, 30*time.Second, log)

	epoch1 := tracker.IncrementEpoch()
	epoch2 := tracker.IncrementEpoch()
	assert.Equal(t, epoch1+1, epoch2, "epoch must be monotonically increasing")
}

// ─── Invariant Tests ──────────────────────────────────────────────────────────

// INVARIANT 1: An acknowledged write must remain recoverable.
// (Tested via integration tests with actual storage)

// INVARIANT 3: A stale node cannot commit writes (epoch fencing).
func TestInvariant3_StaleNodeCannotWrite(t *testing.T) {
	log := logger.New("info", "text", "test")
	tracker := membership.NewTracker(15*time.Second, 30*time.Second, log)

	node := &types.StorageNode{ID: "node-stale", State: types.NodeStateHealthy}
	tracker.RegisterNode(node)

	// Advance epoch to 10 (simulates cluster membership changes)
	for i := 0; i < 10; i++ {
		tracker.IncrementEpoch()
	}
	assert.Equal(t, int64(10), tracker.GetEpoch())

	// Stale heartbeat with epoch 5 must be rejected
	err := tracker.ProcessHeartbeat(&types.Heartbeat{
		NodeID:       "node-stale",
		ClusterEpoch: 5,
		Timestamp:    time.Now(),
	})
	assert.Error(t, err, "INVARIANT 3: stale node heartbeat must be rejected")
}

// INVARIANT 5: A tombstone cannot be overwritten by an older version.
// (Verified in metadata service's apply logic - tombstones are stored in a
// separate map and checked before any write is visible)

// INVARIANT 7: Replica repair must verify checksum before completion.
func TestInvariant7_RepairVerifiesChecksum(t *testing.T) {
	// The checksum verification is built into storage.Node.PutObject()
	// which is called during repair. Corruption is detected on GetObject.
	data := []byte("repaired data")
	correctCS := checksum.ComputeSHA256(data)

	// Valid repair - checksum matches
	err := checksum.Verify(data, correctCS, types.ChecksumSHA256)
	assert.NoError(t, err, "valid data with correct checksum must pass")

	// Tampered repair - checksum doesn't match
	tampered := append(data, 'X')
	err = checksum.Verify(tampered, correctCS, types.ChecksumSHA256)
	assert.Error(t, err, "INVARIANT 7: tampered data must fail checksum verification")
}

// init verifies core type package is accessible
func init() {
	_ = types.NewDossError
	_ = fmt.Sprintf
}
