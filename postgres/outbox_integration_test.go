//go:build integration

// Outbox + relay integration test. It exercises the full M3 path against real
// infrastructure: a transfer written through the PG store lands an unpublished
// outbox row in the SAME transaction (atomicity), and the relay then ships that
// row to a real Kafka broker (redpanda) exactly as the production wiring would.
//
// It reuses the postgres container, pool, and store stood up by TestMain in
// conformance_test.go, and adds a throwaway redpanda container for the broker.
// Run with: go test -tags=integration ./postgres
package postgres_test

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/internal/outbox"
)

// resetOutbox clears the outbox alongside the ledger tables so this test starts
// from a clean slate regardless of what the conformance suite left behind (those
// transfers now write outbox rows too).
func resetOutbox(t *testing.T) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(),
		`TRUNCATE outbox, entries, transfers, holds, accounts CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// countUnpublished returns how many outbox rows still await publication.
func countUnpublished(t *testing.T) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	return n
}

// startRedpanda boots a throwaway redpanda broker and returns its seed address.
func startRedpanda(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v23.3.3")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("kafka seed broker: %v", err)
	}
	return broker
}

// createTopic provisions a single-partition topic up front so the consumer can
// read partition 0 deterministically (rather than racing auto-creation).
func createTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer cc.Close()
	if err := cc.CreateTopics(kafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

// TestPG_Outbox_AtomicWithTransfer proves the event row is written in the same
// transaction as the money movement.
func TestPG_Outbox_AtomicWithTransfer(t *testing.T) {
	resetOutbox(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)

	tr, err := transfer(t, "k1", "alice", "bob", 250)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}

	// Exactly one unpublished row, describing exactly this transfer.
	var (
		id, eventType, aggregateID string
		payload                    []byte
		publishedAt                *time.Time
	)
	if err := testPool.QueryRow(context.Background(),
		`SELECT id, event_type, aggregate_id, payload, published_at FROM outbox`).
		Scan(&id, &eventType, &aggregateID, &payload, &publishedAt); err != nil {
		t.Fatalf("select outbox row (want exactly one): %v", err)
	}
	if eventType != "transfer.posted" {
		t.Errorf("event_type = %q, want transfer.posted", eventType)
	}
	if aggregateID != tr.ID {
		t.Errorf("aggregate_id = %q, want transfer id %q", aggregateID, tr.ID)
	}
	if publishedAt != nil {
		t.Errorf("published_at = %v, want NULL before the relay runs", *publishedAt)
	}
	var got ledger.Transfer
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("payload not valid transfer JSON: %v", err)
	}
	if got.ID != tr.ID || got.Amount != 250 || got.FromAccountID != "alice" || got.ToAccountID != "bob" {
		t.Errorf("payload mismatch: %+v", got)
	}
	if n := countUnpublished(t); n != 1 {
		t.Errorf("unpublished outbox rows = %d, want 1", n)
	}
}

// TestPG_Outbox_RelayPublishesToKafka drives the full path: apply a transfer,
// assert one unpublished row, run the relay against a real Kafka publisher, assert
// the row is marked published and the event is readable from Kafka with the right
// payload, then assert a second Drain is a no-op (idempotent).
func TestPG_Outbox_RelayPublishesToKafka(t *testing.T) {
	resetOutbox(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)

	broker := startRedpanda(t)
	const topic = "ledger.transfers"
	createTopic(t, broker, topic)

	tr, err := transfer(t, "k1", "alice", "bob", 250)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if n := countUnpublished(t); n != 1 {
		t.Fatalf("before drain: unpublished = %d, want 1", n)
	}

	pub := outbox.NewKafkaPublisher([]string{broker}, topic)
	defer pub.Close()
	relay := outbox.NewRelay(testPool, pub)

	ctx := context.Background()
	n, err := relay.Drain(ctx, 10)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("Drain published %d, want 1", n)
	}
	if u := countUnpublished(t); u != 0 {
		t.Errorf("after drain: unpublished = %d, want 0 (row stamped published)", u)
	}

	// The event is durably on the broker with the expected key/value/headers.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read kafka message: %v", err)
	}
	if string(msg.Key) != tr.ID {
		t.Errorf("kafka key = %q, want aggregate id %q", msg.Key, tr.ID)
	}
	var got ledger.Transfer
	if err := json.Unmarshal(msg.Value, &got); err != nil {
		t.Fatalf("kafka value not valid transfer JSON: %v", err)
	}
	if got.ID != tr.ID || got.Amount != 250 {
		t.Errorf("kafka payload mismatch: %+v", got)
	}
	if h := header(msg, "event_type"); h != "transfer.posted" {
		t.Errorf("event_type header = %q, want transfer.posted", h)
	}
	if h := header(msg, "event_id"); h == "" {
		t.Errorf("event_id header missing (consumers dedupe on it)")
	}

	// A second drain finds nothing new — the stamp made publication idempotent.
	n2, err := relay.Drain(ctx, 10)
	if err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	if n2 != 0 {
		t.Errorf("second Drain published %d, want 0", n2)
	}
}

// TestPG_Outbox_CaptureEmitsEvent proves a hold capture also lands a
// transfer.posted event in its transaction.
func TestPG_Outbox_CaptureEmitsEvent(t *testing.T) {
	resetOutbox(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, "a", "alice", "bob", 300, 0, now)
	tr, err := testStore.Capture(context.Background(),
		ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300}, now)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}

	var aggregateID, eventType string
	if err := testPool.QueryRow(context.Background(),
		`SELECT aggregate_id, event_type FROM outbox WHERE published_at IS NULL`).
		Scan(&aggregateID, &eventType); err != nil {
		t.Fatalf("select outbox row: %v", err)
	}
	if eventType != "transfer.posted" || aggregateID != tr.ID {
		t.Errorf("capture event = %q/%q, want transfer.posted/%s", eventType, aggregateID, tr.ID)
	}
	if n := countUnpublished(t); n != 1 {
		t.Errorf("unpublished after capture = %d, want 1", n)
	}
}

// header returns the first Kafka header value for key, or "" if absent.
func header(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
