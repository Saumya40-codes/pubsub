package main

import (
	"fmt"
	"math/rand"
	"strconv"
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

	fmt.Println("\n")

	// Create consumers and subscribe them to topics
	consumerGroups := map[string][]string{
		"stock-price-update": {"price-analyzer", "alert-generator"},
		"stock-news-update":  {"news-aggregator", "sentiment-analyzer"},
		"trade-execution":    {"trade-processor", "order-verifier"},
		"portfolio-update":   {"portfolio-manager"},
	}

	var groupId int = 0

	for topicName, groups := range consumerGroups {
		for _, name := range groups {
			consumer := core_pubsub.CreateConsumer(name, strconv.Itoa(groupId))
			err := consumer.Subscribe(consumer, topicName)
			if err != nil {
				fmt.Println("Error subscribing:", err)
				continue
			}
			go consumer.Run()

			fmt.Println("==============SUBSCRIBE=======================")
			fmt.Printf("Consumer %s in Consumer Group %d subscribed to %s\n", name, groupId, topicName)
			fmt.Println("=============================================")
		}

		groupId++
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
				var partitionIndex int = rand.Intn(topics[topicName]) // randomly select a partition

				messageContent := fmt.Sprintf("Message regarding %s from %s", topicName, producerName)
				message := core_pubsub.CreateMessage(topicName, messageContent, partitionIndex)

				err := producer.Publish(topicName, message)
				if err != nil {
					fmt.Println("Error publishing message:", err)
					continue
				}

				fmt.Println("==============PUBLISH=======================\n", "Published message to", topicName, "\n============================================")

				// Simulate a delay between messages
				time.Sleep(time.Second * 4)
			}
		}(topicName, producerName)
	}

	// Prevent the main function from exiting immediately
	select {}
}
