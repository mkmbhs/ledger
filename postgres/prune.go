package postgres

import (
	"context"
	"time"
)

// PruneStats reports what one retention run did.
type PruneStats struct {
	TransferKeysNulled int64
	HoldKeysNulled     int64
	OutboxRowsDeleted  int64
}

// Prune applies the retention policy (migrations/0005): idempotency keys on
// rows older than keyRetention are NULLed — never those of active holds — and
// published outbox rows older than outboxRetention are deleted. Ledger rows
// (transfers, entries, holds) are never deleted.
//
// The contract this creates: a retry arriving after its key was pruned is a
// NEW operation — a duplicate. Keep keyRetention longer than the longest
// possible client retry horizon, and never retry a request older than it.
func (s *Store) Prune(ctx context.Context, keyRetention, outboxRetention time.Duration) (PruneStats, error) {
	var st PruneStats
	err := s.pool.QueryRow(ctx,
		`SELECT * FROM ledger_prune(make_interval(secs => $1), make_interval(secs => $2))`,
		keyRetention.Seconds(), outboxRetention.Seconds()).
		Scan(&st.TransferKeysNulled, &st.HoldKeysNulled, &st.OutboxRowsDeleted)
	return st, err
}
