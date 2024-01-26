package kafka_confluent

import (
	"context"
	"fmt"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
	"log"
)

const (
	defaultSessionTimeout = 6000
	pollTimeout           = 100
	autoOffsetReset       = "earliest"
)

// Consumer interface
type Consumer interface {
	ConsumeMessage(ctx context.Context, topic string, msg proto.Message) (proto.Message, *confluentKafka.Message, error)
	CommitMessage(msg *confluentKafka.Message) error
	Close() error
}

// consumer is a wrapper around confluent-kafka-go Consumer
type consumer struct {
	consumer *confluentKafka.Consumer
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL string, groupID string) (Consumer, error) {
	c, err := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           groupID,
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
		"auto.offset.reset":  autoOffsetReset,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &consumer{
		consumer: c,
	}, nil
}

// ConsumeMessage reads a message from Kafka and unmarshals it into message.
func (c *consumer) ConsumeMessage(ctx context.Context, topic string, msg proto.Message) (proto.Message, *confluentKafka.Message, error) {
	if err := c.consumer.Subscribe(topic, nil); err != nil {
		c.Close()
		return nil, nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			event := c.consumer.Poll(pollTimeout)
			switch e := event.(type) {
			case *confluentKafka.Message:
				if err := proto.Unmarshal(e.Value, msg); err != nil {
					return nil, e, err
				}
				return msg, e, nil
			case confluentKafka.Error:
				log.Printf("kafka error: %v", e)
				// В случае ошибки Kafka продолжаем опрос (или можно выбрать другую стратегию)
				continue
			default:
				continue
			}
		}
	}
}

func (c *consumer) CommitMessage(msg *confluentKafka.Message) error {
	if _, err := c.consumer.CommitMessage(msg); err != nil {
		return err
	}

	return nil
}

// Close closes the consumer.
func (c *consumer) Close() error {
	return c.consumer.Close()
}
