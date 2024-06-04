package core_pubsub

import (
	"crypto/rand"
	"sync"
)

type Consumer struct {
	id         string
	groupId    string
	topics     []string
	active     bool
	messages   chan *Message
	partitions map[string]int
	mu         sync.RWMutex
}

func CreateConsumer(id string, topic string, groupId string) *Consumer {
	return &Consumer{
		id:       generateConsumerId(),
		groupId:  groupId,
		topics:   []string{topic},
		active:   true,
		messages: make(chan *Message, 100),
		mu:       sync.RWMutex{},
	}
}

func generateConsumerId() string {
	bufb := make([]byte, 10)

	_, err := rand.Read(bufb)

	if err != nil {
		panic(err)
	}

	return "sub_" + string(bufb)
}
