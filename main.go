package main

import (
	"fmt"
	"time"

	core_pubsub "github.com/Saumya40-codes/pubsub/core"
)

func main() {
	var location string = "north area"
	fmt.Println("Order received")

	// Creating a broker instance
	broker := core_pubsub.GetorSetBrokerInstance()

	// Defining topics and partitions
	topics := map[string]int{
		"driver-update":     2,
		"food-update":       3,
		"order-update":      2,
		"customer-feedback": 1,
	}

	// Creating topics
	for topicName, partitions := range topics {
		newTopic, err := broker.CreateNewTopic(topicName, partitions)
		if err != nil {
			fmt.Println("Error creating topic:", err)
			continue
		}
		fmt.Println("Created topic:", newTopic.Name)
	}

	// Create consumers and subscribe them to topics
	consumerGroups := map[string][]string{
		"driver-update":     {"delivery-group-near", "delivery-group-most-nearest"},
		"food-update":       {"kitchen-group-main", "kitchen-group-backup"},
		"order-update":      {"order-management", "order-tracking"},
		"customer-feedback": {"feedback-analysis"},
	}

	for topicName, groups := range consumerGroups {
		for _, group := range groups {
			consumer := core_pubsub.CreateConsumer(topicName, group)
			err := consumer.Subscribe(consumer, topicName)
			if err != nil {
				fmt.Println("Error subscribing:", err)
				continue
			}
			go consumer.Run()
			fmt.Println("Consumer group", group, "subscribed to", topicName)
		}
	}

	// Simulate some delay to allow potential race conditions
	time.Sleep(time.Second * 4)

	// Create producers and publish messages
	producers := map[string]string{
		"food-update-producer":       "food-update",
		"driver-update-producer":     "driver-update",
		"order-update-producer":      "order-update",
		"customer-feedback-producer": "customer-feedback",
	}

	for producerName, topicName := range producers {
		producer := core_pubsub.CreateProducer(producerName)

		// Create and publish a message
		var partitionIndex int
		if location == "north area" {
			partitionIndex = 0
		} else {
			partitionIndex = 1
		}
		message := core_pubsub.CreateMessage(topicName, "Message regarding "+topicName, partitionIndex)

		err := producer.Publish(topicName, message)
		if err != nil {
			fmt.Println("Error publishing message:", err)
			continue
		}
		fmt.Println("Published message to", topicName)
	}

	// Waiting for some time to allow potential message processing
	time.Sleep(time.Second * 3)
}
