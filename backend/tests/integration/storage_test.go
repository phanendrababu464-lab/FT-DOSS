// Package tests/integration provides integration tests for the storage node.
package tests_integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ft-doss/backend/internal/checksum"
	"github.com/ft-doss/backend/internal/storage"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNode(t *testing.T, nodeID string) (*storage.Node, string) {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	walDir := filepath.Join(dir, "wal")
	log := logger.New("error", "text", nodeID)
	node, err := storage.NewNode(nodeID, dataDir, walDir, true, log)
	require.NoError(t, err)
	return node, dir
}

// TestPutAndGet verifies basic write-then-read with checksum verification.
func TestPutAndGet(t *testing.T) {
	node, _ := setupNode(t, "node-1")

	data := []byte("test object content for distributed storage")
	cs := checksum.ComputeSHA256(data)

	meta := &storage.ObjectMeta{
		Bucket:    "test-bucket",
		Key:       "test-key",
		VersionID: "v1",
		Size:      int64(len(data)),
		Checksum:  cs,
		CreatedAt: time.Now(),
	}

	// Write
	err := node.PutObject(meta, data, 0)
	require.NoError(t, err)

	// Read back
	gotData, gotMeta, err := node.GetObject("test-bucket", "test-key", "v1")
	require.NoError(t, err)
	assert.Equal(t, data, gotData)
	assert.Equal(t, cs, gotMeta.Checksum)
}

// TestChecksumVerifiedOnRead ensures on-disk corruption is caught when reading.
// INVARIANT: A corrupt replica cannot be the authoritative source.
func TestChecksumVerifiedOnRead(t *testing.T) {
	node, tmpDir := setupNode(t, "node-1")

	data := []byte("clean data")
	cs := checksum.ComputeSHA256(data)
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Size:      int64(len(data)),
		Checksum:  cs,
		CreatedAt: time.Now(),
	}
	err := node.PutObject(meta, data, 0)
	require.NoError(t, err)

	// Directly corrupt the file on disk
	dataFile := filepath.Join(tmpDir, "data", "bucket", "key", "v1.dat")
	raw, err := os.ReadFile(dataFile)
	require.NoError(t, err)
	raw[0] ^= 0xFF
	require.NoError(t, os.WriteFile(dataFile, raw, 0644))

	// Read must detect corruption
	_, _, err = node.GetObject("bucket", "key", "v1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHECKSUM_MISMATCH", "must detect on-disk corruption")
}

// TestEpochFencing verifies writes from stale epochs are rejected.
// INVARIANT 3: A stale node cannot commit writes.
func TestEpochFencing(t *testing.T) {
	node, _ := setupNode(t, "node-1")
	node.UpdateEpoch(10)

	data := []byte("fenced data")
	cs := checksum.ComputeSHA256(data)
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Size:      int64(len(data)),
		Checksum:  cs,
		CreatedAt: time.Now(),
	}

	// Write with stale epoch 5 (current is 10) must fail
	err := node.PutObject(meta, data, 5)
	require.Error(t, err)
	dossErr, ok := err.(*types.DossError)
	require.True(t, ok)
	assert.Equal(t, types.ErrStaleEpoch, dossErr.Code)
}

// TestDeleteAndTombstone verifies object deletion removes data.
func TestDeleteAndTombstone(t *testing.T) {
	node, _ := setupNode(t, "node-1")

	data := []byte("delete me")
	cs := checksum.ComputeSHA256(data)
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Checksum:  cs,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}
	require.NoError(t, node.PutObject(meta, data, 0))

	// Delete
	require.NoError(t, node.DeleteObject("bucket", "key", "v1", 0))

	// Read must fail after deletion
	_, _, err := node.GetObject("bucket", "key", "v1")
	require.Error(t, err)
}

// TestUnavailableNodeRejectsWrites verifies node state machine.
func TestUnavailableNodeRejectsWrites(t *testing.T) {
	node, _ := setupNode(t, "node-1")
	node.SetState(types.NodeStateUnavailable)

	data := []byte("data")
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Checksum:  checksum.ComputeSHA256(data),
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}

	err := node.PutObject(meta, data, 0)
	require.Error(t, err)
	dossErr, ok := err.(*types.DossError)
	require.True(t, ok)
	assert.Equal(t, types.ErrNodeUnavailable, dossErr.Code)
}

// TestInjectCorruption verifies the chaos lab corruption tool works.
func TestInjectCorruption(t *testing.T) {
	node, _ := setupNode(t, "node-1")

	data := []byte("integrity-protected data")
	cs := checksum.ComputeSHA256(data)
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Checksum:  cs,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}
	require.NoError(t, node.PutObject(meta, data, 0))

	// Inject corruption
	require.NoError(t, node.InjectCorruption("bucket", "key", "v1"))

	// Next read must detect corruption
	_, _, err := node.GetObject("bucket", "key", "v1")
	require.Error(t, err, "corruption should be detected on read")
}

// TestScrubDetectsCorruption verifies the scrubber finds corrupt objects.
func TestScrubDetectsCorruption(t *testing.T) {
	node, _ := setupNode(t, "node-1")

	data := []byte("scrub test data")
	cs := checksum.ComputeSHA256(data)
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Checksum:  cs,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}
	require.NoError(t, node.PutObject(meta, data, 0))
	require.NoError(t, node.InjectCorruption("bucket", "key", "v1"))

	// Scrub should detect corruption
	corrupt, _, err := node.ScrubAll()
	require.NoError(t, err)
	assert.Len(t, corrupt, 1, "scrub must detect the corrupted object")
}

// TestMultipleVersionsIsolated verifies different versions don't interfere.
func TestMultipleVersionsIsolated(t *testing.T) {
	node, _ := setupNode(t, "node-1")

	data1 := []byte("version one")
	data2 := []byte("version two")

	for _, tc := range []struct {
		data      []byte
		versionID string
	}{
		{data1, "v1"},
		{data2, "v2"},
	} {
		cs := checksum.ComputeSHA256(tc.data)
		meta := &storage.ObjectMeta{
			Bucket:    "bucket",
			Key:       "myobj",
			VersionID: tc.versionID,
			Checksum:  cs,
			Size:      int64(len(tc.data)),
			CreatedAt: time.Now(),
		}
		require.NoError(t, node.PutObject(meta, tc.data, 0))
	}

	got1, _, err := node.GetObject("bucket", "myobj", "v1")
	require.NoError(t, err)
	assert.Equal(t, data1, got1)

	got2, _, err := node.GetObject("bucket", "myobj", "v2")
	require.NoError(t, err)
	assert.Equal(t, data2, got2)
}

// TestWALDurability verifies WAL entries are written before data is ACK'd.
func TestWALDurability(t *testing.T) {
	node, dir := setupNode(t, "node-1")

	data := []byte("wal test")
	cs := checksum.ComputeSHA256(data)
	meta := &storage.ObjectMeta{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Checksum:  cs,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}
	require.NoError(t, node.PutObject(meta, data, 0))

	// WAL file must exist and be non-empty
	walFile := filepath.Join(dir, "wal", "wal.jsonl")
	info, err := os.Stat(walFile)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "WAL must have entries after write")
}

// Compile check for context package
var _ = context.Background
