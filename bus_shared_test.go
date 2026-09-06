package bus

import (
	"sync"
	"testing"

	"webtyp.com/binary"
)

func testBus(t *testing.T) {
	b := New()
	defer b.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex
	received := make([]string, 0)

	wg.Add(2)
	h := func(msg binary.Message) {
		mu.Lock()
		received = append(received, string(msg.Payload))
		mu.Unlock()
		wg.Done()
	}

	sub1 := b.Subscribe("test", h)
	sub2 := b.Subscribe("test", h)

	if sub1.Topic() != "test" {
		t.Errorf("expected topic test, got %s", sub1.Topic())
	}

	msg := binary.Message{
		Topic:   "test",
		Payload: []byte("hello"),
	}

	if err := b.Publish("test", msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	wg.Wait()

	if len(received) != 2 {
		t.Errorf("expected 2 messages, got %d", len(received))
	}

	// Test Topics
	topics := b.Topics()
	if len(topics) != 1 || topics[0] != "test" {
		t.Errorf("expected topics [test], got %v", topics)
	}

	// Test Cancel
	sub1.Cancel()
	wg.Add(1)
	if err := b.Publish("test", msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	wg.Wait()

	if len(received) != 3 {
		t.Errorf("expected 3 messages total, got %d", len(received))
	}

	sub2.Cancel()
	topics = b.Topics()
	if len(topics) != 0 {
		t.Errorf("expected 0 topics, got %v", topics)
	}
}
