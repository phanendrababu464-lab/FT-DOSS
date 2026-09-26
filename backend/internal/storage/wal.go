// Package storage - Write-Ahead Log for the storage node.
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALEntry is a single WAL record.
type WALEntry struct {
	Sequence  int64     `json:"seq"`
	Operation string    `json:"op"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	VersionID string    `json:"version_id"`
	Checksum  string    `json:"checksum,omitempty"`
	Size      int64     `json:"size,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// WAL is a write-ahead log for durability guarantees.
// Every write is first appended to the WAL before data is committed.
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	path     string
	sequence int64
}

// NewWAL creates or opens a WAL file.
func NewWAL(walDir string) (*WAL, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(walDir, "wal.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening WAL: %w", err)
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, 65536),
		path:   path,
	}, nil
}

// Write appends an entry to the WAL and flushes/syncs to durable storage.
// This guarantees durability before acknowledging the write.
func (w *WAL) Write(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sequence++
	entry.Sequence = w.sequence

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling WAL entry: %w", err)
	}

	if _, err := w.writer.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("writing WAL entry: %w", err)
	}

	// Flush and sync for durability
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads all WAL entries for crash recovery.
func (w *WAL) Replay() ([]WALEntry, error) {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var entries []WALEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 10*1024*1024), 10*1024*1024)
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue // skip corrupt WAL entries
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
