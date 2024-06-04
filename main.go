package main

import (
	"fmt"
	"math/rand"
	"time"

	core_pubsub "github.com/Saumya40-codes/pubsub/core"
)

func main() {
	fmt.Println("Stock Monitoring System")

	// Creating a broker instance
	broker := core_pubsub.GetorSetBrokerInstance()

	// Defining topics and partitions
	topics := map[string]int{
		"stock-price-update": 3,
		"stock-news-update":  2,
		"trade-execution":    2,
		"portfolio-update":   1,
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
		"stock-price-update": {"price-analyzer", "alert-generator"},
		"stock-news-update":  {"news-aggregator", "sentiment-analyzer"},
		"trade-execution":    {"trade-processor", "order-verifier"},
		"portfolio-update":   {"portfolio-manager"},
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

			fmt.Println("==============SUBSCRIBE=======================")
			fmt.Println("Consumer group", group, "subscribed to", topicName)
			fmt.Println("=============================================")
		}
	}

	// Simulate some delay to allow potential race conditions
	time.Sleep(time.Second * 4)

	// Create producers and publish messages
	producers := map[string]string{
		"stock-price-update-producer": "stock-price-update",
		"stock-news-update-producer":  "stock-news-update",
		"trade-execution-producer":    "trade-execution",
		"portfolio-update-producer":   "portfolio-update",
	}

	for producerName, topicName := range producers {
		producer := core_pubsub.CreateProducer(producerName)

		go func(topicName string, producerName string) {
			for {
				// Create and publish a message
				var partitionIndex int = rand.Intn(topics[topicName])

				messageContent := fmt.Sprintf("Message regarding %s from %s", topicName, producerName)
				message := core_pubsub.CreateMessage(topicName, messageContent, partitionIndex)

				err := producer.Publish(topicName, message)
				if err != nil {
					fmt.Println("Error publishing message:", err)
					continue
				}

				fmt.Println("==============PUBLISH=======================")
				fmt.Println("Published message to", topicName)
				fmt.Println("============================================")

				// Simulate a delay between messages
				time.Sleep(time.Second * 4)
			}
		}(topicName, producerName)
	}

	// Prevent the main function from exiting immediately
	select {}
}
