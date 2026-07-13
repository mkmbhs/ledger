package postgres

import (
	"context"
	"fmt"
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
	// Refused here and again inside ledger_prune: a non-positive retention
	// would strip idempotency protection from everything, instantly.
	if keyRetention <= 0 || outboxRetention <= 0 {
		return PruneStats{}, fmt.Errorf("postgres: prune retentions must be positive (key=%s, outbox=%s)", keyRetention, outboxRetention)
	}
	var st PruneStats
	err := s.pool.QueryRow(ctx,
		`SELECT * FROM ledger_prune(make_interval(secs => $1), make_interval(secs => $2))`,
		keyRetention.Seconds(), outboxRetention.Seconds()).
		Scan(&st.TransferKeysNulled, &st.HoldKeysNulled, &st.OutboxRowsDeleted)
	return st, err
}
