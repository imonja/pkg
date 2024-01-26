package kafka_sarama

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
	"log"
	"strings"
)

const (
	autoOffsetReset = sarama.OffsetOldest
)

// Consumer interface
type Consumer interface {
	ConsumeMessage(
		ctx context.Context,
		message proto.Message,
		topic string,
		handleMessage func(message proto.Message, consumeMessage *sarama.ConsumerMessage) error,
	) error
	Close() error
}

// consumer is a wrapper around sarama.Consumer
type consumer struct {
	consumer sarama.ConsumerGroup
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL string, groupID string) (Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = autoOffsetReset
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaURL}, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return &consumer{
		consumer: consumerGroup,
	}, nil
}

// ConsumeMessage reads a message from Kafka and unmarshals it into message.
func (c *consumer) ConsumeMessage(
	ctx context.Context,
	message proto.Message,
	topic string,
	handleMessage func(message proto.Message, consumeMessage *sarama.ConsumerMessage) error,
) error {
	consumerHandler := consumerMessageHandler{
		ready:         make(chan bool),
		message:       message,
		handleMessage: handleMessage,
	}

	go func() {
		defer close(consumerHandler.ready)
		for {
			if err := c.consumer.Consume(ctx, strings.Split(topic, ","), &consumerHandler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumerHandler.ready = make(chan bool)
		}
	}()

	<-consumerHandler.ready
	log.Println("Sarama consumer up and running!...")

	<-ctx.Done()
	return nil
}

// Close closes the consumer group and releases any associated resources.
func (c *consumer) Close() error {
	return c.consumer.Close()
}

type consumerMessageHandler struct {
	ready         chan bool
	message       proto.Message
	handleMessage func(message proto.Message, consumeMessage *sarama.ConsumerMessage) error
}

func (c *consumerMessageHandler) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *consumerMessageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerMessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if err := c.handleMessage(c.message, message); err != nil {
			log.Printf("Message handling failed: %v", err)
			return err
		}
		log.Println(fmt.Sprintf("Message claimed: ", "topic", message.Topic, "message", message))
		// TODO: create manual commit
		session.MarkMessage(message, "")
	}
	return nil
}
