package core_pubsub

import (
	"math"
	"sync"
)

type Topic struct {
	Name       string
	partitions int
	consumers  []*Consumer
	mu         sync.RWMutex
}

func CreateTopic(name string, partitions int) *Topic {
	return &Topic{
		Name:       name,
		partitions: partitions,
		consumers:  make([]*Consumer, 0),
		mu:         sync.RWMutex{},
	}
}

// topic will have partitions, each partition will have a consumer group
// each consumer group will have a consumer
// each consumer might be associated with multiple partitions or one partition
func (t *Topic) AddConsumer(c *Consumer) (partitionIndex []int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	consumerGroup := t.getConsumerByGroupId(c.groupId)
	t.consumers = append(t.consumers, c)
	consumerGroup = append(consumerGroup, c)

	if len(consumerGroup) == 1 {

		for i := 0; i < t.partitions; i++ {
			partitionIndex = append(partitionIndex, i)
		}

		// fmt.Println("partitionIndex: ", partitionIndex)

		c.partitions[t.Name] = partitionIndex
		return partitionIndex
	}

	partitionToFirstConsumer := math.Ceil(float64(t.partitions) / float64(math.Max(float64(len(consumerGroup)), float64(t.partitions))))
	partitionToOtherConsumer := (float64(t.partitions) / float64(math.Max(float64(len(consumerGroup)), float64(t.partitions))))

	// fmt.Println("partitionToFirstConsumer: ", partitionToFirstConsumer)
	// fmt.Println("partitionToOtherConsumer: ", partitionToOtherConsumer)
	// fmt.Println("ConsumerGroup: ", consumerGroup)

	var pIndex int = 0

	for idx, consumer := range consumerGroup {

		var partitionIndex []int

		if idx != 0 {
			for i := 0; i < int(partitionToOtherConsumer); i++ {
				partitionIndex = append(partitionIndex, pIndex)
				pIndex++
			}
		} else {
			for i := 0; i < int(partitionToFirstConsumer); i++ {
				partitionIndex = append(partitionIndex, pIndex)
				pIndex++
			}
		}

		consumer.partitions[t.Name] = partitionIndex
	}

	return c.partitions[t.Name]
}

func (t *Topic) getConsumerByGroupId(groupId string) []*Consumer {
	var consumers []*Consumer

	for _, consumer := range t.consumers {
		if consumer.groupId == groupId {
			consumers = append(consumers, consumer)
		}
	}

	return consumers
}
