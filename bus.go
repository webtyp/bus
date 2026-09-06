package bus

import (
	"sort"
	"sync"

	"webtyp.com/binary"
)

type Bus interface {
	// Subscribe registers a handler for a topic.
	// Returns a Subscription handle to cancel the registration.
	Subscribe(topic string, handler func(msg binary.Message)) Subscription

	// Publish sends a message to all subscribers of a topic.
	Publish(topic string, msg binary.Message) error

	// Topics returns a sorted list of all active topics.
	Topics() []string

	// Close shuts down the bus and clears all registrations.
	Close() error
}

type Subscription interface {
	Topic() string
	Cancel()
}

// Internal: slices, not maps (TinyGo binary size + simplicity)
type topicEntry struct {
	topic string
	subs  []subscriber
}

type subscriber struct {
	id      uint32
	handler func(msg binary.Message)
}

type bus struct {
	mu     sync.RWMutex
	topics []topicEntry // O(n) scan — fine for typical topic counts
	nextID uint32
}

// New creates a new in-memory bus.
func New() Bus {
	return &bus{}
}

func (b *bus) Subscribe(topic string, handler func(msg binary.Message)) Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id := b.nextID
	sub := subscriber{id: id, handler: handler}

	found := false
	for i := range b.topics {
		if b.topics[i].topic == topic {
			b.topics[i].subs = append(b.topics[i].subs, sub)
			found = true
			break
		}
	}

	if !found {
		b.topics = append(b.topics, topicEntry{
			topic: topic,
			subs:  []subscriber{sub},
		})
	}

	return &subscription{
		bus:   b,
		topic: topic,
		id:    id,
	}
}

func (b *bus) Publish(topic string, msg binary.Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, entry := range b.topics {
		if entry.topic == topic {
			for _, sub := range entry.subs {
				go sub.handler(msg)
			}
			break
		}
	}
	return nil
}

func (b *bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.topics))
	for _, entry := range b.topics {
		if len(entry.subs) > 0 {
			topics = append(topics, entry.topic)
		}
	}
	sort.Strings(topics)
	return topics
}

func (b *bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics = nil
	return nil
}

type subscription struct {
	bus   *bus
	topic string
	id    uint32
}

func (s *subscription) Topic() string {
	return s.topic
}

func (s *subscription) Cancel() {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()

	for i := range s.bus.topics {
		if s.bus.topics[i].topic == s.topic {
			subs := s.bus.topics[i].subs
			for j := range subs {
				if subs[j].id == s.id {
					// Remove subscriber
					s.bus.topics[i].subs = append(subs[:j], subs[j+1:]...)
					break
				}
			}
			// Remove topic if no subs left
			if len(s.bus.topics[i].subs) == 0 {
				s.bus.topics = append(s.bus.topics[:i], s.bus.topics[i+1:]...)
			}
			break
		}
	}
}
