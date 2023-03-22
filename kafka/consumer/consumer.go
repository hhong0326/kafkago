package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	Consumer *kafka.Consumer
	Run      bool
}

func (c *Consumer) Init(config *kafka.ConfigMap) error {
	var err error
	c.Consumer, err = kafka.NewConsumer(config)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) Subscribe(topics []string) error {
	err := c.Consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}
	return nil
}
