package postgres

import (
	"context"
	"encoding/json"

	"github.com/mkmbhs/ledger"
)

// eventTransferPosted is emitted whenever a transfer is posted — both by a direct
// ApplyTransfer and by settling a hold in Capture.
const eventTransferPosted = "transfer.posted"

// insertTransferPosted appends a transfer.posted event to the outbox.
//
// THE WHOLE POINT: q here is the SAME pgx.Tx that just inserted the transfer and
// its entries and moved the balances. Because the event row is written inside that
// transaction, it commits atomically with the money movement — there is no window
// where the ledger changed but the event was lost, and no event is ever emitted
// for a transfer that rolled back. A separate relay (internal/outbox) later ships
// the row to Kafka. This is the transactional-outbox cure for the dual-write bug.
//
// The payload is the transfer serialized as JSON (id, accounts, amount, currency,
// status, entries); the outbox row id doubles as the event id consumers dedupe on.
func insertTransferPosted(ctx context.Context, q querier, t ledger.Transfer) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `
		INSERT INTO outbox (id, event_type, aggregate_id, payload)
		VALUES ($1, $2, $3, $4)`,
		newID(), eventTransferPosted, t.ID, payload)
	return err
}
