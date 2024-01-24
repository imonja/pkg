package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

// Producer defines an interface for a Kafka message producer.
type Producer interface {
	ProduceMessage(ctx context.Context, msg proto.Message, topic string) (*kafka.Message, error)
	Close()
}

// producer implements the Producer interface.
type producer struct {
	producer *kafka.Producer
}

// NewProducer creates and returns a new Producer.
func NewProducer(kafkaURL string) (Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaURL,
	})
	if err != nil {
		return nil, err
	}

	return &producer{producer: p}, nil
}

// ProduceMessage sends a Kafka message.
func (p *producer) ProduceMessage(ctx context.Context, msg proto.Message, topic string) (*kafka.Message, error) {
	value, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          value,
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	if err = p.producer.Produce(message, deliveryChan); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e := <-deliveryChan:
		kafkaMessage := e.(*kafka.Message)
		if kafkaMessage.TopicPartition.Error != nil {
			return nil, kafkaMessage.TopicPartition.Error
		}
		return kafkaMessage, nil
	}
}

// Close terminates the producer.
func (p *producer) Close() {
	p.producer.Close()
}
