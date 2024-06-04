package core_pubsub

import "sync"

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
