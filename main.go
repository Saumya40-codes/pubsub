package main

import (
	"fmt"
	"time"

	core_pubsub "github.com/Saumya40-codes/pubsub/core"
)

func main() {
	// Get the broker instance (ensures only one instance)
	broker := core_pubsub.GetorSetBrokerInstance()

	// Create a new topic
	topicName := "test-topic"
	partitions := 2 // adjust partitions as needed
	newTopic, err := broker.CreateNewTopic(topicName, partitions)
	if err != nil {
		fmt.Println("Error creating topic:", err)
		return
	}
	fmt.Println("Created topic:", newTopic.Name)

	// **Test 1: Verify topic exists after creation**

	// Get the topic using the name
	existingTopic, err := broker.GetTopic(topicName)
	if err != nil {
		fmt.Println("Error getting topic:", err)
		return
	}

	if existingTopic != nil {
		fmt.Println("Topic", topicName, "exists!")
	} else {
		fmt.Println("Topic", topicName, "not found (unexpected)")
	}

	// **Test 2: Simulate additional interaction (optional)**

	// Create a consumer (replace with your actual logic)
	consumer := core_pubsub.CreateConsumer(topicName, "test-group")
	consumer1 := core_pubsub.CreateConsumer(topicName, "test-group1")
	// consumer2 := core_pubsub.CreateConsumer(topicName, "test-group")

	// Simulate some delay to allow potential race conditions
	time.Sleep(time.Second * 1)

	// **Test 3: Verify topic is available for subscription (optional)**

	// Subscribe the consumer to the topic (replace with your actual logic)
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

	// err = consumer2.Subscribe(consumer2, topicName)
	// if err != nil {
	// 	fmt.Println("Error subscribing:", err)
	// 	return
	// }

	fmt.Println("Consumer subscribed to", topicName)

	// Run the consumer in a separate goroutine (replace with your actual logic)
	go consumer.Run()

	go consumer1.Run()

	// Wait for some time to allow potential message processing (optional)
	producer := core_pubsub.CreateProducer("test-producer")
	message := core_pubsub.CreateMessage(topicName, "test-message", 0)

	// Publish a message to the topic (replace with your actual logic)
	err = producer.Publish(topicName, message)

	if err != nil {
		fmt.Println("Error publishing message:", err)
		return
	}

	fmt.Println("Published message to", topicName)

	// Wait for some time to allow potential message processing (optional)
	time.Sleep(time.Second * 1)

	// Unsubscribe and close the consumer (replace with your actual logic)
	consumer.Unsubscribe(existingTopic)
	consumer.Deactivate()
}
