package main

import (
	"fmt"
	"time"

	core_pubsub "github.com/Saumya40-codes/pubsub/core"
)

func main() {
	var location string = "north area"
	fmt.Println("Order received from", location)

	// Creating a broker instance
	broker := core_pubsub.GetorSetBrokerInstance()

	// New topic (can be many)
	topicName := "driver-update"
	partitions := 2 // number of partitions in the topic

	newTopic, err := broker.CreateNewTopic(topicName, partitions)
	if err != nil {
		fmt.Println("Error creating topic:", err)
		return
	}
	fmt.Println("Created topic:", newTopic.Name)

	// Creating a consumer instance
	consumer := core_pubsub.CreateConsumer(topicName, "delivery-group-near")
	consumer1 := core_pubsub.CreateConsumer(topicName, "delivery-group-most-nearest")
	consumer2 := core_pubsub.CreateConsumer(topicName, "delivery-group-near")
	consumer3 := core_pubsub.CreateConsumer(topicName, "delivery-group-most-nearest")

	// Simulate some delay to allow potential race conditions
	time.Sleep(time.Second * 1)

	// Subscribe the consumer to the topic
	err = consumer.Subscribe(consumer, topicName)
	if err != nil {
		fmt.Println("Error subscribing:", err)
		return
	}

	err = consumer1.Subscribe(consumer1, topicName)
	if err != nil {
		fmt.Println("Error subscribing:", err)
		return
	}

	err = consumer2.Subscribe(consumer2, topicName)
	if err != nil {
		fmt.Println("Error subscribing:", err)
		return
	}

	err = consumer3.Subscribe(consumer3, topicName)
	if err != nil {
		fmt.Println("Error subscribing:", err)
		return
	}

	fmt.Println("Consumers subscribed to", topicName)

	go consumer.Run()
	go consumer1.Run()
	go consumer2.Run()
	go consumer3.Run()

	// creating a producer instance
	producer := core_pubsub.CreateProducer("food-update-producer")
	// consider partitioning based on location
	// say partition 0 for north area and partition 1 for south area

	// Create a message to publish
	var partitionIndex int
	if location == "north area" {
		partitionIndex = 0
	} else {
		partitionIndex = 1
	}
	message := core_pubsub.CreateMessage(topicName, "order preparation has started", partitionIndex)

	// Publishing a message to the topic
	err = producer.Publish(topicName, message)

	if err != nil {
		fmt.Println("Error publishing message:", err)
		return
	}

	fmt.Println("Published message to", topicName)

	// Waiting for some time to allow potential message processing
	time.Sleep(time.Second * 1)

	// consumer.Unsubscribe(existingTopic)
	// consumer.Deactivate()
}
