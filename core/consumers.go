package core_pubsub

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
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

func CreateConsumer(topic string, groupId string) *Consumer {
	return &Consumer{
		id:         generateConsumerId(),
		groupId:    groupId,
		topics:     []string{topic},
		active:     true,
		messages:   make(chan *Message, 100),
		partitions: map[string][]int{},
		mu:         sync.RWMutex{},
	}
}

func (c *Consumer) Subscribe(consumer *Consumer, topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	broker := GetorSetBrokerInstance()

	t, err := broker.GetTopic(topic)

	if err != nil {
		return errors.New("topic not found")
	}

	consumerinTopic := t.getConsumerByGroupId(c.groupId)

	if len(consumerinTopic)+1 > t.partitions {
		fmt.Println("No partitions available for consumer group: ", c.groupId)
		t.consumers = append(t.consumers, c)
	} else {
		assignedPartitions := t.AddConsumer(c)

		if len(assignedPartitions) == 0 {
			fmt.Println("No partitions assigned to consumer: ", c.id)
		}
	}

	return nil
}

func (c *Consumer) Unsubscribe(topic *Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.topics {
		if t == topic.Name {
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
			fmt.Printf("Consumer %s with groupId: %s, received message: %s\n", c.id, c.groupId, msg.data)
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

func generateConsumerId() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 10)
	for i := range b {
		max := big.NewInt(int64(len(charset)))
		r, err := rand.Int(rand.Reader, max)
		if err != nil {
			panic(err)
		}
		b[i] = charset[r.Int64()]
	}

	return "sub_" + string(b)
}
