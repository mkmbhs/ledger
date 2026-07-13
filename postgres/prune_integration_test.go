//go:build integration

// Retention tests. They pin exactly what ledger_prune (migrations/0005) may
// and may not touch: old idempotency keys are NULLed, active holds and
// unpublished outbox rows are untouchable, ledger rows are never deleted — and
// the documented edge is real: a retry after its key was pruned becomes a new
// operation.
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/ledgertest"
)

// backdate rewrites created_at on the row bearing an idempotency key — test
// scaffolding standing in for the passage of time.
func backdate(t *testing.T, table, key string, age time.Duration) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(),
		`UPDATE `+table+` SET created_at = now() - make_interval(secs => $1) WHERE idempotency_key = $2`,
		age.Seconds(), key)
	if err != nil {
		t.Fatalf("backdate %s %s: %v", table, key, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("backdate %s %s: affected %d rows, want 1", table, key, tag.RowsAffected())
	}
}

func count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestPG_Prune_Retention(t *testing.T) {
	reset(t)
	if _, err := testPool.Exec(context.Background(), `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()

	// History: an old transfer, a fresh transfer, an old captured hold, and an
	// old-but-ACTIVE hold.
	oldTr, err := transfer(t, "k-old", "alice", "bob", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transfer(t, "k-new", "alice", "bob", 50); err != nil {
		t.Fatal(err)
	}
	hTerm := authorize(t, "a-old", "alice", "bob", 30, 0, now)
	if _, err := testStore.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "c-old", HoldID: hTerm.ID, Amount: 30}, now); err != nil {
		t.Fatal(err)
	}
	hLive := authorize(t, "a-live", "alice", "bob", 20, 0, now)

	// Age everything except k-new past the 30-day key retention; a-live is old
	// AND active, so its key must survive on status alone.
	const age40d = 40 * 24 * time.Hour
	backdate(t, "transfers", "k-old", age40d)
	backdate(t, "transfers", "c-old", age40d)
	backdate(t, "holds", "a-old", age40d)
	backdate(t, "holds", "a-live", age40d)

	// Outbox: mark the old transfer's event published 10 days ago (prunable);
	// every other event row stays unpublished (untouchable).
	if _, err := testPool.Exec(ctx,
		`UPDATE outbox SET published_at = now() - interval '10 days' WHERE aggregate_id = $1`, oldTr.ID); err != nil {
		t.Fatal(err)
	}
	unpublishedBefore := 2 // k-new transfer + c-old capture events

	transfersBefore, entriesBefore, holdsBefore := count(t, "transfers"), count(t, "entries"), count(t, "holds")

	st, err := testStore.Prune(ctx, 30*24*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if st.TransferKeysNulled != 2 || st.HoldKeysNulled != 1 || st.OutboxRowsDeleted != 1 {
		t.Errorf("stats = %+v, want 2 transfer keys, 1 hold key, 1 outbox row", st)
	}

	// Ledger rows are immutable: nothing was deleted anywhere.
	if a, b, c := count(t, "transfers"), count(t, "entries"), count(t, "holds"); a != transfersBefore || b != entriesBefore || c != holdsBefore {
		t.Errorf("ledger rows changed: transfers %d->%d entries %d->%d holds %d->%d",
			transfersBefore, a, entriesBefore, b, holdsBefore, c)
	}
	// The pruned transfer still reads back whole — key empty, entries intact.
	got, ok, err := testStore.GetTransfer(ctx, oldTr.ID)
	if err != nil || !ok {
		t.Fatalf("GetTransfer after prune: ok=%v err=%v", ok, err)
	}
	if got.IdempotencyKey != "" || len(got.Entries) != 2 || got.Amount != 100 {
		t.Errorf("pruned transfer = %+v, want empty key with intact entries", got)
	}
	// The fresh key and the active hold's key survived: both still replay.
	if tr, err := transfer(t, "k-new", "alice", "bob", 50); err != nil || tr.Amount != 50 {
		t.Errorf("k-new replay after prune: %v", err)
	}
	if h := authorize(t, "a-live", "alice", "bob", 20, 0, now); h.ID != hLive.ID {
		t.Errorf("a-live replay made a new hold: %s vs %s", h.ID, hLive.ID)
	}
	// Unpublished outbox rows are untouchable, no matter how the clock reads.
	if n := countUnpublished(t); n != unpublishedBefore {
		t.Errorf("unpublished outbox rows = %d, want %d", n, unpublishedBefore)
	}

	// THE DOCUMENTED EDGE: a retry of the pruned key is a NEW operation. This
	// is the contract line — retention must exceed the client retry horizon.
	dup, err := transfer(t, "k-old", "alice", "bob", 100)
	if err != nil {
		t.Fatalf("replay of pruned key: %v", err)
	}
	if dup.ID == oldTr.ID {
		t.Errorf("replay of pruned key returned the original transfer; want a new (duplicate) one")
	}

	// Books still balance around everything above: alice moved 100+50+30+100,
	// still holds 20 for the live hold.
	ledgertest.AssertInvariants(t, testStore, map[string]ledger.Money{"alice": 1000, "bob": 0})

	// Prune is idempotent: a second run finds nothing.
	if st2, err := testStore.Prune(ctx, 30*24*time.Hour, 7*24*time.Hour); err != nil ||
		st2.TransferKeysNulled != 0 || st2.HoldKeysNulled != 0 || st2.OutboxRowsDeleted != 0 {
		t.Errorf("second prune = %+v err=%v, want all zeros", st2, err)
	}
}

// TestPG_Prune_RefusesNonPositiveRetention pins the guard at both layers: a
// zero or negative retention is a typo that would strip idempotency protection
// from everything, and it is refused before touching a row.
func TestPG_Prune_RefusesNonPositiveRetention(t *testing.T) {
	ctx := context.Background()
	if _, err := testStore.Prune(ctx, 0, time.Hour); err == nil {
		t.Error("Prune with zero key retention succeeded, want error")
	}
	if _, err := testStore.Prune(ctx, time.Hour, -time.Minute); err == nil {
		t.Error("Prune with negative outbox retention succeeded, want error")
	}
	// The SQL function refuses too, independently of the Go wrapper.
	if _, err := testPool.Exec(ctx, `SELECT * FROM ledger_prune('0 seconds', '1 day')`); err == nil {
		t.Error("ledger_prune with zero retention succeeded, want error")
	}
}
