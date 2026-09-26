// Package observability implements Prometheus metrics and real-time SSE event bus.
package observability

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EventType defines system event names.
type EventType string

const (
	EventNodeJoined         EventType = "NODE_JOINED"
	EventNodeRemoved        EventType = "NODE_REMOVED"
	EventNodeStatusChanged  EventType = "NODE_STATUS_CHANGED"
	EventNodeCrashed        EventType = "NODE_CRASHED"
	EventNodeUnavailable    EventType = "NODE_UNAVAILABLE"
	EventNodeRecovered      EventType = "NODE_RECOVERED"

	EventPutObject          EventType = "PUT_OBJECT"
	EventGetObject          EventType = "GET_OBJECT"
	EventDeleteObject       EventType = "DELETE_OBJECT"
	EventTombstoneWritten   EventType = "TOMBSTONE_WRITTEN"

	EventReplicaCreated     EventType = "REPLICA_CREATED"
	EventWriteQuorumReached EventType = "WRITE_QUORUM_REACHED"
	EventReadQuorumReached  EventType = "READ_QUORUM_REACHED"

	EventWALAppend          EventType = "WAL_APPEND"
	EventWALFsync           EventType = "WAL_FSYNC"

	EventChecksumVerified   EventType = "CHECKSUM_VERIFIED"
	EventChecksumMismatch   EventType = "CHECKSUM_MISMATCH"
	EventScrubStarted       EventType = "SCRUB_STARTED"
	EventScrubCompleted     EventType = "SCRUB_COMPLETED"

	EventRepairQueued       EventType = "REPAIR_QUEUED"
	EventRepairStarted      EventType = "REPAIR_STARTED"
	EventRepairCompleted    EventType = "REPAIR_COMPLETED"
	EventRepairFailed       EventType = "REPAIR_FAILED"

	EventRaftLeaderChanged  EventType = "RAFT_LEADER_CHANGED"
	EventRaftTermChanged    EventType = "RAFT_TERM_CHANGED"
	EventEpochChanged       EventType = "EPOCH_CHANGED"
	EventStaleEpochRejected EventType = "STALE_EPOCH_REJECTED"
	EventResetState         EventType = "RESET_STATE"
)

// Event represents a single operational system event.
type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Severity  string                 `json:"severity,omitempty"`
	Source    string                 `json:"source,omitempty"`
	NodeID    string                 `json:"nodeId,omitempty"`
	ObjectKey string                 `json:"objectKey,omitempty"`
	PGID      string                 `json:"pgId,omitempty"`
	Message   string                 `json:"message,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// EventBus provides thread-safe publish-subscribe for SSE streaming.
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
}

// Global default event bus
var defaultEventBus = NewEventBus()

// DefaultEventBus returns the global shared EventBus.
func DefaultEventBus() *EventBus {
	return defaultEventBus
}

// NewEventBus initializes a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[chan Event]struct{}),
	}
}

// Subscribe returns a channel of events and an unsubscribe cleanup function.
func (b *EventBus) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 256)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		if _, ok := b.subscribers[ch]; ok {
			delete(b.subscribers, ch)
			close(ch)
		}
		b.mu.Unlock()
	}

	return ch, unsubscribe
}

// Publish distributes an event to all subscribers in a non-blocking manner.
func (b *EventBus) Publish(evt Event) {
	if evt.ID == "" {
		evt.ID = fmt.Sprintf("evt-%s", uuid.New().String()[:8])
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.subscribers {
		select {
		case ch <- evt:
		default:
			// Non-blocking write: if subscriber buffer is full, drop to prevent blocking storage path
		}
	}
}

// SubscriberCount returns active SSE subscriber count.
func (b *EventBus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}
