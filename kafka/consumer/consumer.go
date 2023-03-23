package consumer

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	KafkaConsumer *kafka.Consumer
	Run           bool
}

func (c *Consumer) Init(config *kafka.ConfigMap) error {
	var err error
	c.KafkaConsumer, err = kafka.NewConsumer(config)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) Subscribe(topics []string) error {
	err := c.KafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) ReadMessageConsumer() error {
	c.Run = true

	fmt.Println("Consumer is running...")

	for c.Run {
		msg, err := c.KafkaConsumer.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Key is %s\n", string(msg.Key))
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return err
		}
	}
	return nil
}

func (c *Consumer) PollConsumer() error {
	c.Run = true
	var err error
	for c.Run {
		ev := c.KafkaConsumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Key is %s\n", string(e.Key))
			fmt.Printf("Message on %s: %s\n", e.TopicPartition, string(e.Value))
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			c.Run = false
			err = e
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
	return err
}
