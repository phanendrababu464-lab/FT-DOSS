package observability

import (
	"sync"
	"testing"
	"time"
)

func TestEventBus_PublishSubscribe(t *testing.T) {
	bus := NewEventBus()

	ch, unsubscribe := bus.Subscribe()
	defer unsubscribe()

	if bus.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", bus.SubscriberCount())
	}

	evt := Event{
		Type:    EventPutObject,
		Message: "Test event",
	}

	bus.Publish(evt)

	select {
	case received := <-ch:
		if received.Type != EventPutObject {
			t.Errorf("expected type %s, got %s", EventPutObject, received.Type)
		}
		if received.Message != "Test event" {
			t.Errorf("expected message 'Test event', got '%s'", received.Message)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestEventBus_MultipleSubscribers(t *testing.T) {
	bus := NewEventBus()

	ch1, unsub1 := bus.Subscribe()
	defer unsub1()

	ch2, unsub2 := bus.Subscribe()
	defer unsub2()

	if bus.SubscriberCount() != 2 {
		t.Fatalf("expected 2 subscribers, got %d", bus.SubscriberCount())
	}

	evt := Event{Type: EventNodeCrashed, NodeID: "storage-1"}
	bus.Publish(evt)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		e := <-ch1
		if e.NodeID != "storage-1" {
			t.Errorf("ch1 expected node storage-1, got %s", e.NodeID)
		}
	}()

	go func() {
		defer wg.Done()
		e := <-ch2
		if e.NodeID != "storage-1" {
			t.Errorf("ch2 expected node storage-1, got %s", e.NodeID)
		}
	}()

	wg.Wait()
}
