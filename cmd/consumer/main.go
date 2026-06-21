// Command consumer is a standalone, illustrative downstream consumer for the
// ledger's transfer events. It reads the Kafka topic the outbox relay publishes
// to (see internal/outbox) and pretty-prints one line per event, so a reviewer
// can watch the producer→broker→consumer path end to end.
//
// It pairs with the repo's `docker compose up`, which runs the producer side
// (the ledger API + the outbox relay shipping events to Kafka). Typical loop:
//
//	docker compose up -d                       # brokers + API + relay
//	go run ./cmd/consumer                       # this consumer, tailing events
//	curl -XPOST localhost:8080/transfers ...    # POST a transfer over REST
//	# → a [transfer.posted] line appears here moments later
//
// Example invocation with explicit flags:
//
//	go run ./cmd/consumer \
//	  -brokers=localhost:9092 -topic=ledger.transfers -group=ledger-example-consumer
//
// It is a demo, not production: it logs and skips bad messages rather than
// dead-lettering them, and it relies on the consumer group to commit offsets.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// transferEvent mirrors the JSON the producer writes as the message value. The
// relay publishes the outbox payload verbatim, and that payload is a
// ledger.Transfer marshaled with encoding/json and no struct tags — so the field
// names here must match the Go field names exactly (ID, FromAccountID, ...).
// Only the fields we print are listed; unknown fields (IdempotencyKey, Entries)
// are simply ignored by json.Unmarshal.
type transferEvent struct {
	ID            string    `json:"ID"`
	FromAccountID string    `json:"FromAccountID"`
	ToAccountID   string    `json:"ToAccountID"`
	Amount        int64     `json:"Amount"` // minor units (e.g. cents); see ledger.Money
	Currency      string    `json:"Currency"`
	Status        string    `json:"Status"`
	CreatedAt     time.Time `json:"CreatedAt"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka bootstrap brokers")
	topic := flag.String("topic", "ledger.transfers", "topic to consume transfer events from")
	group := flag.String("group", "ledger-example-consumer", "consumer group id (commits offsets)")
	flag.Parse()

	// Cancel the context on Ctrl-C / SIGTERM so the read loop unblocks and we can
	// close the reader cleanly. signal.NotifyContext restores default signal
	// handling on stop().
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// GroupID turns on consumer-group semantics: the broker assigns partitions and
	// ReadMessage auto-commits offsets, so a restart resumes where we left off.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(*brokers, ","),
		Topic:   *topic,
		GroupID: *group,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("closing reader: %v", err)
		}
	}()

	log.Printf("consuming topic %q from %s as group %q (Ctrl-C to stop)",
		*topic, *brokers, *group)

	var consumed int
	for {
		// ReadMessage blocks until a message arrives or ctx is cancelled. On
		// cancellation it returns ctx.Err(), which breaks the loop for shutdown.
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // shutting down
			}
			// Transient read error: log and retry rather than crash the loop.
			log.Printf("read error: %v", err)
			continue
		}

		eventType := header(msg.Headers, "event_type")
		eventID := header(msg.Headers, "event_id")

		var t transferEvent
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			// Malformed payload: report and move on. The offset still commits, so a
			// poison message can't wedge the group — fine for a demo, whereas a real
			// consumer would route it to a dead-letter topic.
			log.Printf("skipping malformed message (key=%s event_id=%s): %v",
				string(msg.Key), eventID, err)
			continue
		}

		// One clean line per event, e.g.:
		// [transfer.posted] id=9cb2f1a0 250 USD 3f8e... -> a17c... (event_id=c33b...)
		log.Printf("[%s] id=%s %d %s %s -> %s (event_id=%s)",
			eventType, short(t.ID), t.Amount, t.Currency,
			short(t.FromAccountID), short(t.ToAccountID), short(eventID))
		consumed++
	}

	log.Printf("shutting down: consumed %d events", consumed)
}

// header returns the value of the first header with the given key, or "" if
// absent. Kafka headers are an ordered slice, not a map.
func header(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// short trims a long opaque id to its first 8 characters for readable output,
// appending an ellipsis when truncated. Empty stays empty.
func short(id string) string {
	const n = 8
	if len(id) <= n {
		return id
	}
	return id[:n] + "..."
}
