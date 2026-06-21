// Package outbox ships rows from the ledger's transactional outbox table to a
// message broker.
//
// The Postgres Store writes a domain event into the outbox in the same
// transaction that moves the money (see internal/store/postgres), so the event is
// durable the instant the transfer commits. This package is the other half: a
// Relay polls the unpublished tail and hands each event to a Publisher (Kafka in
// production, a fake in tests).
//
// Delivery is AT-LEAST-ONCE by design. The relay publishes a row and THEN stamps
// published_at; a crash in between leaves the row unpublished, so the next Drain
// re-publishes it. The broker may therefore see an event more than once, and
// consumers MUST dedupe on the event id (Event.ID, the outbox primary key). The
// alternative ordering — mark first, then publish — would be at-most-once and
// could silently drop events, which is unacceptable for money. We choose
// duplicates over loss.
package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one outbox row awaiting publication. ID is the dedupe key consumers
// key on; Payload is the raw JSON written by the Store.
type Event struct {
	ID          string
	EventType   string
	AggregateID string
	Payload     []byte
}

// Publisher delivers an event to the broker. It is the seam that keeps Kafka out
// of the relay's core logic, so the broker is swappable and the Drain loop is
// testable with a fake. Publish must return a non-nil error if delivery is not
// durable; the relay then leaves the row unpublished for a later retry.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Relay drains the outbox to a Publisher.
type Relay struct {
	pool *pgxpool.Pool
	pub  Publisher
}

// NewRelay returns a Relay that drains the given pool's outbox via pub.
func NewRelay(pool *pgxpool.Pool, pub Publisher) *Relay {
	return &Relay{pool: pool, pub: pub}
}

// Drain publishes up to batchSize unpublished events, oldest first, and stamps
// each as published. It returns the number successfully published-and-stamped.
//
// The selection uses FOR UPDATE SKIP LOCKED: it row-locks the batch it grabs and
// SKIPs any rows another relay worker already locked. That is what lets several
// relay workers run concurrently for throughput — each grabs a disjoint batch
// instead of every worker fighting over (and double-publishing) the same head
// rows. The locks are held until this transaction commits.
//
// Ordering within the transaction is publish-then-stamp (see the package doc):
// each event is delivered, then its row is stamped in the same transaction. If a
// publish fails mid-batch, the stamps already applied for the rows delivered
// before it are committed (those really were published), and the failing row plus
// the rest stay unpublished for the next Drain. Hence at-least-once.
func (r *Relay) Drain(ctx context.Context, batchSize int) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	rows, err := tx.Query(ctx, `
		SELECT id, event_type, aggregate_id, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1`, batchSize)
	if err != nil {
		return 0, err
	}
	// Read the whole batch before publishing: a single pooled connection cannot
	// run the stamping UPDATEs while these rows are still open.
	var batch []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.EventType, &e.AggregateID, &e.Payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	published := 0
	for _, e := range batch {
		// Publish FIRST. A crash here re-publishes on the next Drain (at-least-once).
		if err := r.pub.Publish(ctx, e); err != nil {
			// Keep the stamps for rows already delivered, then surface the failure.
			if cerr := commit(ctx, tx); cerr != nil {
				return published, cerr
			}
			return published, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE outbox SET published_at = now() WHERE id = $1`, e.ID); err != nil {
			return published, err
		}
		published++
	}
	if err := commit(ctx, tx); err != nil {
		return published, err
	}
	return published, nil
}

// commit is a tiny helper so the two commit sites read the same.
func commit(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }
