package outbox

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher delivers outbox events to a Kafka topic via segmentio/kafka-go.
//
// Each event becomes one message: the key is the aggregate id, the value is the
// JSON payload, and the event id and type ride along as headers. Keying by
// aggregate id matters twice over — it routes every event for the same aggregate
// to one partition (so a consumer sees them in order), and it gives the consumer
// the dedupe id (also in the event_id header) it needs to absorb the at-least-once
// duplicates the relay can produce.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher returns a Publisher writing to topic on the given brokers.
//
// RequireAll waits for all in-sync replicas to ack before Publish returns, so a
// "published" event is durable on the broker; only then will the relay stamp the
// row. Auto topic creation is enabled for convenience in dev/test — in production
// the topic is provisioned ahead of time with the right partitioning.
func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // partition by key (aggregate id)
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			// The relay publishes synchronously, one event at a time, so each
			// WriteMessages call must flush immediately. The writer's defaults
			// (BatchSize 100, BatchTimeout 1s) are tuned for fire-and-forget
			// batching and would park every single-message write on the batch
			// timeout — capping the relay at about one event per second.
			BatchSize: 1,
		},
	}
}

// Publish writes one event as a Kafka message. It returns once the broker has
// acked (RequireAll), so a nil error means the event is durably stored.
func (p *KafkaPublisher) Publish(ctx context.Context, e Event) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.AggregateID),
		Value: e.Payload,
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte(e.ID)},
			{Key: "event_type", Value: []byte(e.EventType)},
		},
	})
}

// Close flushes and releases the underlying writer.
func (p *KafkaPublisher) Close() error { return p.writer.Close() }
