//go:build integration

// PostgreSQL integration tests. They need a real database and are gated behind
// the `integration` build tag, so the default `go test ./...` never touches
// Docker. Run with:
//
//	go test -tags=integration ./postgres
//
// A throwaway postgres:16 container is started once via testcontainers-go and
// the migrations are applied. The store-agnostic behavior comes from the
// exported conformance suite (ledgertest) — the same scenarios the in-memory
// reference passes — run here against a schema truncated before each scenario.
// The tests that stay in this file are the ones only PostgreSQL can answer:
// the database-level balance backstop (deferred constraint trigger) that holds
// even against direct SQL bypassing the application.
package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/ledgertest"
	pgstore "github.com/mkmbhs/ledger/postgres"
)

var (
	testPool  *pgxpool.Pool
	testStore *pgstore.Store
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("ledger"),
		postgres.WithUsername("ledger"),
		postgres.WithPassword("ledger"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres container:", err)
		os.Exit(1)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection string:", err)
		_ = testcontainers.TerminateContainer(container)
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open pool:", err)
		_ = testcontainers.TerminateContainer(container)
		os.Exit(1)
	}

	if err := applyMigration(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "apply migration:", err)
		pool.Close()
		_ = testcontainers.TerminateContainer(container)
		os.Exit(1)
	}

	testPool = pool
	testStore = pgstore.New(pool)

	code := m.Run()

	pool.Close()
	_ = testcontainers.TerminateContainer(container)
	os.Exit(code)
}

// applyMigration loads every migrations/*.sql file from the repo root, in sorted
// (numeric) order, and executes each. Passing a whole multi-statement script with
// no arguments uses pgx's simple query protocol, which runs every statement in one
// round trip. All migrations must be applied here: the store writes outbox rows
// (0002) in its money-moving transactions, and the balance backstop trigger
// (0003) is itself under test.
func applyMigration(ctx context.Context, pool *pgxpool.Pool) error {
	paths, err := filepath.Glob(filepath.Join("..", "migrations", "*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return err
		}
	}
	return nil
}

// --- helpers -----------------------------------------------------------------

func reset(t *testing.T) {
	t.Helper()
	// The outbox is truncated too: money-moving scenarios write outbox rows, and
	// each scenario should start from a fully empty schema.
	if _, err := testPool.Exec(context.Background(),
		`TRUNCATE outbox, entries, transfers, holds, accounts CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func createAccount(t *testing.T, id, ccy string, opening ledger.Money) {
	t.Helper()
	if err := testStore.CreateAccount(context.Background(),
		ledger.Account{ID: id, Currency: ccy, Balance: opening}); err != nil {
		t.Fatalf("CreateAccount(%s): %v", id, err)
	}
}

func transfer(t *testing.T, key, from, to string, amt ledger.Money) (ledger.Transfer, error) {
	t.Helper()
	return testStore.ApplyTransfer(context.Background(), ledger.TransferRequest{
		IdempotencyKey: key, FromAccountID: from, ToAccountID: to, Amount: amt,
	})
}

func authorize(t *testing.T, key, from, to string, amt ledger.Money, ttl time.Duration, now time.Time) ledger.Hold {
	t.Helper()
	h, err := testStore.Authorize(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: key, FromAccountID: from, ToAccountID: to, Amount: amt, ExpiresIn: ttl,
	}, now)
	if err != nil {
		t.Fatalf("Authorize(%s): %v", key, err)
	}
	return h
}

// --- tests -------------------------------------------------------------------

// TestPG_Conformance runs the store-agnostic conformance suite against the
// PostgreSQL store: exactly the scenarios the in-memory reference passes. Each
// scenario starts from a truncated (empty) schema.
func TestPG_Conformance(t *testing.T) {
	ledgertest.Run(t, func(t *testing.T) ledger.Store {
		reset(t)
		return testStore
	})
}

// TestPG_DB_RefusesUnbalancedEntries proves the database-level backstop: even
// bypassing the application entirely, a transaction whose entries do not sum to
// zero per transfer is refused at COMMIT by the deferred constraint trigger.
func TestPG_DB_RefusesUnbalancedEntries(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	ctx := context.Background()

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status)
		VALUES ('t-bad', 'k-bad', 'alice', 'bob', 100, 'USD', 'posted')`); err != nil {
		t.Fatalf("insert transfer: %v", err)
	}
	// A lone debit with no matching credit: money would vanish.
	if _, err := tx.Exec(ctx, `
		INSERT INTO entries (id, transfer_id, account_id, amount)
		VALUES ('e-bad', 't-bad', 'alice', -100)`); err != nil {
		t.Fatalf("insert entry: %v", err)
	}
	err = tx.Commit(ctx)
	if err == nil {
		t.Fatal("commit of unbalanced entries succeeded, want rejection by trigger")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23000" {
		t.Errorf("commit err = %v, want integrity_constraint_violation (23000)", err)
	}

	// The rejected transaction left nothing behind.
	var n int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers WHERE id = 't-bad'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("unbalanced transfer row survived the rolled-back commit")
	}
}

// TestPG_DB_RefusesUnbalancingDelete proves the backstop also catches an
// unbalancing mutation of existing rows: deleting one side of a posted transfer
// is refused at COMMIT.
func TestPG_DB_RefusesUnbalancingDelete(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	ctx := context.Background()

	tr, err := transfer(t, "k1", "alice", "bob", 250)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM entries WHERE transfer_id = $1 AND amount < 0`, tr.ID); err != nil {
		t.Fatalf("delete entry: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("commit deleting one entry succeeded, want rejection by trigger")
	}

	// Both entries survived.
	entries, err := testStore.AccountEntries(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Amount != -250 {
		t.Errorf("alice entries after refused delete = %+v, want the original debit", entries)
	}
	ledgertest.AssertInvariants(t, testStore, map[string]ledger.Money{"alice": 1000, "bob": 0})
}
