// Package checksum provides integrity verification for FT-DOSS objects and chunks.
package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/ft-doss/backend/pkg/types"
)

// Compute calculates the checksum of data using the specified algorithm.
func Compute(data []byte, algo types.ChecksumAlgorithm) (string, error) {
	switch algo {
	case types.ChecksumSHA256, "":
		h := sha256.Sum256(data)
		return fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:])), nil
	case types.ChecksumMD5:
		// MD5 not recommended for integrity; fallback to SHA-256
		h := sha256.Sum256(data)
		return fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:])), nil
	default:
		h := sha256.Sum256(data)
		return fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:])), nil
	}
}

// ComputeSHA256 computes a raw SHA-256 checksum string.
func ComputeSHA256(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:]))
}

// ComputeReader computes checksum from an io.Reader without loading all into RAM.
func ComputeReader(r io.Reader, algo types.ChecksumAlgorithm) (string, int64, error) {
	var h hash.Hash
	var prefix string

	switch algo {
	case types.ChecksumSHA256, "":
		h = sha256.New()
		prefix = "sha256"
	default:
		h = sha256.New()
		prefix = "sha256"
	}

	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, fmt.Errorf("computing checksum: %w", err)
	}

	return fmt.Sprintf("%s:%s", prefix, hex.EncodeToString(h.Sum(nil))), n, nil
}

// ComputeFile computes checksum of a file on disk.
func ComputeFile(path string, algo types.ChecksumAlgorithm) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening file %s: %w", path, err)
	}
	defer f.Close()

	cs, _, err := ComputeReader(f, algo)
	return cs, err
}

// Verify checks whether the provided checksum matches the data.
func Verify(data []byte, expected string, algo types.ChecksumAlgorithm) error {
	actual, err := Compute(data, algo)
	if err != nil {
		return fmt.Errorf("computing checksum for verification: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

// VerifyFile checks whether the file on disk matches the expected checksum.
func VerifyFile(path, expected string, algo types.ChecksumAlgorithm) error {
	actual, err := ComputeFile(path, algo)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("checksum mismatch for %s: expected %s, got %s", path, expected, actual)
	}
	return nil
}

// ChunkManifest holds per-chunk checksums for a large object.
type ChunkManifest struct {
	ObjectID    string       `json:"object_id"`
	VersionID   string       `json:"version_id"`
	TotalSize   int64        `json:"total_size"`
	ChunkSize   int64        `json:"chunk_size"`
	Chunks      []ChunkEntry `json:"chunks"`
	ObjectChecksum string    `json:"object_checksum"`
	Algorithm   string       `json:"algorithm"`
}

// ChunkEntry holds metadata about one chunk.
type ChunkEntry struct {
	Index    int    `json:"index"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

// BuildManifest creates a chunk manifest for a byte slice.
func BuildManifest(objectID, versionID string, data []byte, chunkSizeBytes int64) (*ChunkManifest, error) {
	manifest := &ChunkManifest{
		ObjectID:  objectID,
		VersionID: versionID,
		TotalSize: int64(len(data)),
		ChunkSize: chunkSizeBytes,
		Algorithm: "sha256",
	}

	// Compute per-chunk checksums
	offset := int64(0)
	idx := 0
	for offset < int64(len(data)) {
		end := offset + chunkSizeBytes
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[offset:end]
		cs := ComputeSHA256(chunk)
		manifest.Chunks = append(manifest.Chunks, ChunkEntry{
			Index:    idx,
			Offset:   offset,
			Size:     int64(len(chunk)),
			Checksum: cs,
		})
		offset = end
		idx++
	}

	// Compute full-object checksum
	manifest.ObjectChecksum = ComputeSHA256(data)
	return manifest, nil
}

// VerifyManifest validates all chunk checksums in a manifest.
func VerifyManifest(manifest *ChunkManifest, data []byte) error {
	if manifest.ObjectChecksum != ComputeSHA256(data) {
		return fmt.Errorf("object checksum mismatch")
	}
	for _, chunk := range manifest.Chunks {
		end := chunk.Offset + chunk.Size
		if end > int64(len(data)) {
			return fmt.Errorf("chunk %d exceeds data bounds", chunk.Index)
		}
		cs := ComputeSHA256(data[chunk.Offset:end])
		if cs != chunk.Checksum {
			return fmt.Errorf("chunk %d checksum mismatch: expected %s got %s",
				chunk.Index, chunk.Checksum, cs)
		}
	}
	return nil
}
