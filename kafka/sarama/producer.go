package kafka_sarama

import (
	"context"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

// Producer defines an interface for a Kafka message producer.
type Producer interface {
	ProduceMessage(ctx context.Context, msg proto.Message, topic string) (partition int32, offset int64, err error)
	Close() error
}

// producer wraps a sarama SyncProducer
type producer struct {
	producer sarama.SyncProducer
}

// NewProducer creates and returns a new Producer using sarama.
func NewProducer(kafkaURL string) (Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // Important for the SyncProducer
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Create the SyncProducer
	p, err := sarama.NewSyncProducer([]string{kafkaURL}, config)
	if err != nil {
		return nil, err
	}

	return &producer{producer: p}, nil
}

// ProduceMessage sends a Kafka message using sarama.
func (p *producer) ProduceMessage(ctx context.Context, msg proto.Message, topic string) (partition int32, offset int64, err error) {
	// Marshal the protobuf message
	value, err := proto.Marshal(msg)
	if err != nil {
		return 0, 0, err
	}

	// Construct a ProducerMessage (sarama's message type)
	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	default:
		// Produce the message and wait for reply in the same call
		partition, offset, err = p.producer.SendMessage(kafkaMessage)
		if err != nil {
			return 0, 0, err
		}

		return partition, offset, nil
	}
}

// Close terminates the producer and closes all connections.
func (p *producer) Close() error {
	return p.producer.Close()
}
