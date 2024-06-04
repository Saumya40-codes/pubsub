package core_pubsub

import "sync"

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
