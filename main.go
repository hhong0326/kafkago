package main

import (
	"fmt"
	"kafkago/kafka/consumer"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("Hello, World!")

	// 환경변수 로드
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// init kafka
	c := &consumer.Consumer{}
	config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_HOST") + ":" + os.Getenv("KAFKA_PORT"),
		"group.id":          "chat-group",
		"auto.offset.reset": "earliest",
	}
	err = c.Init(config)
	if err != nil {
		panic(err)
	}

	err = c.Subscribe([]string{"test-A"})
	if err != nil {
		panic(err)
	}

	// consumer
	// run becomes a signal handler to set this to false to break the loop
	c.Run = true

	fmt.Println("Consumer is running...")

	for c.Run {
		msg, err := c.Consumer.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Key is %s\n", string(msg.Key))
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Consumer.Close()
}
