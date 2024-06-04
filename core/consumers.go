package core_pubsub

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
)

type Consumer struct {
	id         string
	groupId    string
	topics     []string
	active     bool
	messages   chan *Message
	partitions map[string][]int
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

func (c *Consumer) Subscribe(topic *Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if topic.partitions < len(topic.consumers)+1 {
		return errors.New("all partitions are already assigned")
	}

	assignedPartitions := topic.AddConsumer(c)

	if len(assignedPartitions) == 0 {
		return errors.New("no partitions assigned")
	}

	return nil
}

func (c *Consumer) Unsubscribe(topic *Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.topics {
		if t == topic.name {
			delete(c.partitions, t)
			return nil
		}
	}

	return errors.New("topic not found")
}

func (c *Consumer) Deactivate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = false
	close(c.messages)
}

func (c *Consumer) Run() {
	for {
		if msg, ok := <-c.messages; ok {
			// process the message
			fmt.Printf("Consumer %s received message: %s\n", c.id, msg.data)
		}
	}
}

func (c *Consumer) OnMessage(msg *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active {
		c.messages <- msg
	}
}
