package core_pubsub

import (
	"errors"
	"sync"
)

type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

func CreateBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
		mu:     sync.RWMutex{},
	}
}

func (b *Broker) CreateNewTopic(name string, partitions int) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[name]; ok {
		return nil, errors.New("topic already exists")
	}

	topic := CreateTopic(name, partitions)
	b.topics[name] = topic

	return topic, nil
}

func (b *Broker) GetTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if topic, ok := b.topics[name]; ok {
		return topic, nil
	}

	return nil, errors.New("topic not found")
}

func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[name]; ok {
		delete(b.topics, name)
		return nil
	}

	return errors.New("topic not found")
}

func (b *Broker) GetTopics() map[string]*Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.topics
}
