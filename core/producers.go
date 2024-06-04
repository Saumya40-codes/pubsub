package core_pubsub

import (
	"errors"
	"sync"
)

type Producer struct {
	id     string
	topics map[string]*Topic
	mu     sync.RWMutex
}

func CreateProducer(id string) *Producer {
	return &Producer{
		id:     id,
		topics: make(map[string]*Topic),
		mu:     sync.RWMutex{},
	}
}

func (p *Producer) Publish(topic string, message *Message) error {
	broker := GetorSetBrokerInstance()

	t, err := broker.GetTopic(topic)

	if err != nil {
		return errors.New("topic not found")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, consumer := range t.consumers {
		partition := consumer.partitions[topic]

		if partition == nil {
			continue
		}

		for _, p := range partition {
			if p == message.partition {
				consumer.messages <- message
				break
			}
		}
	}

	return nil
}
