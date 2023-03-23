package main

import (
	"fmt"
	"kafkago/kafka/consumer"
	"log"
	"os"

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
		"fetch.min.bytes":   1024,
		// "fetch.max.wait.ms": 1000, // not working ㅜㅜ
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
	// err = c.ReadMessageConsumer()
	// if err != nil {
	// 	panic(err)
	// }

	// poll
	err = c.PollConsumer()
	if err != nil {
		panic(err)
	}
}
