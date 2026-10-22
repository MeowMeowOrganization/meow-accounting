// Package kafkaadapter binds the pure settlement worker to kafka-go. The
// adapter alone knows broker transport; accounting stays Kafka-independent.
package kafkaadapter

import (
	"context"
	"fmt"
	"strings"

	"github.com/MeowMeowOrganization/meow-accounting/internal/settlementworker"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	underlying *kafka.Reader
}

func NewReader(brokers []string, topic, groupID string) (*Reader, error) {
	if len(brokers) == 0 || strings.TrimSpace(topic) == "" || strings.TrimSpace(groupID) == "" {
		return nil, fmt.Errorf("Kafka brokers, topic and consumer group must be configured")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("empty Kafka broker")
		}
	}
	return &Reader{underlying: kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // MANDATORY: CommitMessages must be synchronous.
		MaxBytes:       10e6,
	})}, nil
}

func (r *Reader) Fetch(ctx context.Context) (settlementworker.Record, error) {
	message, err := r.underlying.FetchMessage(ctx) // NEVER ReadMessage: auto-commits.
	if err != nil {
		return settlementworker.Record{}, err
	}
	return settlementworker.Record{
		Topic: message.Topic, Partition: message.Partition, Offset: message.Offset,
		Key: message.Key, Value: message.Value,
	}, nil
}

func (r *Reader) Commit(ctx context.Context, record settlementworker.Record) error {
	return r.underlying.CommitMessages(ctx, kafka.Message{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
	})
}

func (r *Reader) Close() error { return r.underlying.Close() }
