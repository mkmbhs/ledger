//go:build integration

// Package postgres conformance suite. These tests need a real PostgreSQL and are
// gated behind the `integration` build tag, so the default `go test ./...` never
// touches a database. Run with:
//
//	go test -tags=integration ./internal/store/postgres
//
// A throwaway postgres:16 container is started once via testcontainers-go, the
// migration is applied, and every test runs against a freshly truncated schema.
// The suite mirrors the in-memory reference's executable spec and additionally
// asserts the ledger invariants (money conserved, balance == opening + sum of
// entries, no negative balances, held <= balance) after each scenario.
package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mkmbhs/ledger/internal/ledger"
	pgstore "github.com/mkmbhs/ledger/internal/store/postgres"
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

// applyMigration loads migrations/0001_init.sql from the repo root and executes
// it. Passing the whole multi-statement script with no arguments uses pgx's
// simple query protocol, which runs every statement in one round trip.
func applyMigration(ctx context.Context, pool *pgxpool.Pool) error {
	path := filepath.Join("..", "..", "..", "migrations", "0001_init.sql")
	sql, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(sql))
	return err
}

// --- helpers -----------------------------------------------------------------

func reset(t *testing.T) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(),
		`TRUNCATE entries, transfers, holds, accounts CASCADE`); err != nil {
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

func getAccount(t *testing.T, id string) ledger.Account {
	t.Helper()
	a, err := testStore.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatalf("GetAccount(%s): %v", id, err)
	}
	return a
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

// assertInvariants checks, for the given accounts, the ledger's safety
// properties: no negative balance, held within balance, and balance equals the
// opening balance plus the sum of the account's entries (reconciliation).
func assertInvariants(t *testing.T, openings map[string]ledger.Money) {
	t.Helper()
	ctx := context.Background()
	var totalBalance, totalOpening ledger.Money
	for id, opening := range openings {
		a := getAccount(t, id)
		if a.Balance < 0 {
			t.Errorf("%s: negative balance %d", id, a.Balance)
		}
		if a.Held < 0 || a.Held > a.Balance {
			t.Errorf("%s: held=%d out of range for balance=%d", id, a.Held, a.Balance)
		}
		entries, err := testStore.AccountEntries(ctx, id)
		if err != nil {
			t.Fatalf("AccountEntries(%s): %v", id, err)
		}
		var sum ledger.Money
		for _, e := range entries {
			sum += e.Amount
		}
		if opening+sum != a.Balance {
			t.Errorf("%s: balance %d != opening %d + sum(entries) %d", id, a.Balance, opening, sum)
		}
		totalBalance += a.Balance
		totalOpening += opening
	}
	if totalBalance != totalOpening {
		t.Errorf("money not conserved: total balance %d != total opening %d", totalBalance, totalOpening)
	}
}

// --- tests -------------------------------------------------------------------

func TestPG_Transfer_Basic(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)

	tr, err := transfer(t, "k1", "alice", "bob", 250)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if tr.Status != ledger.StatusPosted {
		t.Errorf("status = %q, want posted", tr.Status)
	}
	if len(tr.Entries) != 2 || tr.Entries[0].Amount+tr.Entries[1].Amount != 0 {
		t.Errorf("entries not balanced: %+v", tr.Entries)
	}
	if a := getAccount(t, "alice"); a.Balance != 750 {
		t.Errorf("alice balance = %d, want 750", a.Balance)
	}
	if b := getAccount(t, "bob"); b.Balance != 250 {
		t.Errorf("bob balance = %d, want 250", b.Balance)
	}

	// GetTransfer round-trips with entries.
	got, ok, err := testStore.GetTransfer(context.Background(), tr.ID)
	if err != nil || !ok {
		t.Fatalf("GetTransfer: ok=%v err=%v", ok, err)
	}
	if got.Amount != 250 || len(got.Entries) != 2 {
		t.Errorf("GetTransfer mismatch: %+v", got)
	}

	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Transfer_Idempotent(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)

	req := ledger.TransferRequest{IdempotencyKey: "same", FromAccountID: "alice", ToAccountID: "bob", Amount: 100}
	first, err := testStore.ApplyTransfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := testStore.ApplyTransfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Errorf("idempotent retry produced a different transfer: %s vs %s", first.ID, second.ID)
	}
	if a := getAccount(t, "alice"); a.Balance != 900 {
		t.Errorf("alice = %d, want 900 (applied once)", a.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Transfer_IdempotencyConflict(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	createAccount(t, "carol", "USD", 0)

	if _, err := transfer(t, "dup", "alice", "bob", 100); err != nil {
		t.Fatal(err)
	}
	// Same key, different destination -> conflict.
	if _, err := transfer(t, "dup", "alice", "carol", 100); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Errorf("err = %v, want ErrIdempotencyConflict", err)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0, "carol": 0})
}

// TestPG_Transfer_ConcurrentSameKey fires many concurrent retries of one
// idempotency key and asserts the transfer is applied exactly once.
func TestPG_Transfer_ConcurrentSameKey(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)

	const n = 64
	req := ledger.TransferRequest{IdempotencyKey: "retry", FromAccountID: "alice", ToAccountID: "bob", Amount: 100}

	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, err := testStore.ApplyTransfer(context.Background(), req)
			ids[i], errs[i] = tr.ID, err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d got a different transfer id %q vs %q", i, ids[i], ids[0])
		}
	}
	if a := getAccount(t, "alice"); a.Balance != 900 {
		t.Errorf("alice = %d, want 900 (applied once despite %d retries)", a.Balance, n)
	}
	if b := getAccount(t, "bob"); b.Balance != 100 {
		t.Errorf("bob = %d, want 100", b.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

// TestPG_Transfer_ConcurrentConservation runs many distinct transfers between
// opposite directions concurrently and proves money is conserved and no balance
// goes negative — exercising the consistent lock ordering against deadlocks.
func TestPG_Transfer_ConcurrentConservation(t *testing.T) {
	reset(t)
	const accounts = 8
	const opening ledger.Money = 1000
	openings := make(map[string]ledger.Money, accounts)
	for i := range accounts {
		id := fmt.Sprintf("acc-%d", i)
		createAccount(t, id, "USD", opening)
		openings[id] = opening
	}

	var wg sync.WaitGroup
	const rounds = 200
	for i := range rounds {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from := fmt.Sprintf("acc-%d", i%accounts)
			to := fmt.Sprintf("acc-%d", (i+1)%accounts)
			// Opposite-direction transfers on the same pair would deadlock without
			// the sorted lock order; ErrInsufficientFunds is a valid outcome.
			_, _ = transfer(t, fmt.Sprintf("c-%d", i), from, to, 1)
		}(i)
	}
	wg.Wait()

	assertInvariants(t, openings)
}

func TestPG_Transfer_InsufficientFunds(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 100)
	createAccount(t, "bob", "USD", 0)

	if _, err := transfer(t, "k", "alice", "bob", 500); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("err = %v, want ErrInsufficientFunds", err)
	}
	if a := getAccount(t, "alice"); a.Balance != 100 {
		t.Errorf("alice balance changed on failed transfer: %d", a.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 100, "bob": 0})
}

func TestPG_Transfer_Validation(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "euro", "EUR", 0)

	if _, err := transfer(t, "x", "alice", "ghost", 10); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Errorf("missing dest err = %v, want ErrAccountNotFound", err)
	}
	if _, err := transfer(t, "y", "alice", "euro", 10); !errors.Is(err, ledger.ErrCurrencyMismatch) {
		t.Errorf("currency err = %v, want ErrCurrencyMismatch", err)
	}
}

func TestPG_AccountEntries_Reconciliation(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)

	for i, amt := range []ledger.Money{100, 50, 25} {
		if _, err := transfer(t, fmt.Sprintf("h-%d", i), "alice", "bob", amt); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := testStore.AccountEntries(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Errorf("alice entries = %d, want 3", len(entries))
	}
	var sum ledger.Money
	for _, e := range entries {
		sum += e.Amount
	}
	if sum != -175 {
		t.Errorf("alice entry sum = %d, want -175", sum)
	}
	if _, err := testStore.AccountEntries(context.Background(), "ghost"); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Errorf("unknown account err = %v, want ErrAccountNotFound", err)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Authorize_ReservesWithoutMoving(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, "a1", "alice", "bob", 300, 0, now)
	if h.Status != ledger.HoldActive {
		t.Errorf("status = %q, want active", h.Status)
	}
	a := getAccount(t, "alice")
	if a.Balance != 1000 || a.Held != 300 || a.Available() != 700 {
		t.Errorf("alice balance=%d held=%d available=%d, want 1000/300/700", a.Balance, a.Held, a.Available())
	}
	if b := getAccount(t, "bob"); b.Balance != 0 {
		t.Errorf("bob received money before capture: %d", b.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Authorize_Idempotent_And_Conflict(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h1 := authorize(t, "same", "alice", "bob", 300, 0, now)
	h2 := authorize(t, "same", "alice", "bob", 300, 0, now)
	if h1.ID != h2.ID {
		t.Errorf("idempotent authorize made two holds")
	}
	if a := getAccount(t, "alice"); a.Held != 300 {
		t.Errorf("held = %d, want 300 (reserved once)", a.Held)
	}
	// Same key, different amount -> conflict.
	if _, err := testStore.Authorize(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: "same", FromAccountID: "alice", ToAccountID: "bob", Amount: 999,
	}, now); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Errorf("err = %v, want ErrIdempotencyConflict", err)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Authorize_InsufficientAvailable(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	authorize(t, "a1", "alice", "bob", 700, 0, now) // available now 300
	if _, err := testStore.Authorize(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: "a2", FromAccountID: "alice", ToAccountID: "bob", Amount: 400,
	}, now); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("err = %v, want ErrInsufficientFunds", err)
	}
}

func TestPG_Capture_Full(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, "a", "alice", "bob", 300, 0, now)
	tr, err := testStore.Capture(context.Background(), ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300}, now)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if tr.Amount != 300 {
		t.Errorf("transfer amount = %d, want 300", tr.Amount)
	}
	a := getAccount(t, "alice")
	if a.Balance != 700 || a.Held != 0 {
		t.Errorf("alice balance=%d held=%d, want 700/0", a.Balance, a.Held)
	}
	if b := getAccount(t, "bob"); b.Balance != 300 {
		t.Errorf("bob = %d, want 300", b.Balance)
	}
	if hold, _, _ := testStore.GetHold(context.Background(), h.ID); hold.Status != ledger.HoldCaptured || hold.Captured != 300 || hold.CaptureTransferID != tr.ID {
		t.Errorf("hold = %q/%d/%s, want captured/300/%s", hold.Status, hold.Captured, hold.CaptureTransferID, tr.ID)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Capture_Partial_ReleasesRemainder(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, "a", "alice", "bob", 300, 0, now)
	if _, err := testStore.Capture(context.Background(), ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 100}, now); err != nil {
		t.Fatal(err)
	}
	a := getAccount(t, "alice")
	// Only 100 moved; the 200 remainder is released back to available.
	if a.Balance != 900 || a.Held != 0 || a.Available() != 900 {
		t.Errorf("alice balance=%d held=%d available=%d, want 900/0/900", a.Balance, a.Held, a.Available())
	}
	if b := getAccount(t, "bob"); b.Balance != 100 {
		t.Errorf("bob = %d, want 100", b.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Capture_Idempotent_Concurrent(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, "a", "alice", "bob", 300, 0, now)
	req := ledger.CaptureRequest{IdempotencyKey: "cap", HoldID: h.ID, Amount: 300}

	const n = 32
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, err := testStore.Capture(context.Background(), req, now)
			ids[i], errs[i] = tr.ID, err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d captured a different transfer", i)
		}
	}
	if b := getAccount(t, "bob"); b.Balance != 300 {
		t.Errorf("bob = %d, want 300 (captured exactly once)", b.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_Capture_Errors(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	h := authorize(t, "a", "alice", "bob", 300, 0, now)
	if _, err := testStore.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "x", HoldID: h.ID, Amount: 400}, now); !errors.Is(err, ledger.ErrCaptureExceedsHold) {
		t.Errorf("over-capture err = %v, want ErrCaptureExceedsHold", err)
	}
	if _, err := testStore.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "y", HoldID: "ghost", Amount: 10}, now); !errors.Is(err, ledger.ErrHoldNotFound) {
		t.Errorf("unknown hold err = %v, want ErrHoldNotFound", err)
	}
	if _, err := testStore.Void(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "z", HoldID: h.ID, Amount: 100}, now); !errors.Is(err, ledger.ErrHoldNotActive) {
		t.Errorf("capture-after-void err = %v, want ErrHoldNotActive", err)
	}
}

func TestPG_Void_ReleasesFunds_And_DoubleVoid(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	h := authorize(t, "a", "alice", "bob", 300, 0, now)
	vh, err := testStore.Void(ctx, h.ID)
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if vh.Status != ledger.HoldVoided {
		t.Errorf("status = %q, want voided", vh.Status)
	}
	if a := getAccount(t, "alice"); a.Held != 0 || a.Available() != 1000 {
		t.Errorf("alice not fully released: held=%d", a.Held)
	}
	// Double void is a no-op and must not double-release.
	if _, err := testStore.Void(ctx, h.ID); err != nil {
		t.Fatalf("second Void: %v", err)
	}
	if a := getAccount(t, "alice"); a.Held != 0 || a.Balance != 1000 {
		t.Errorf("double Void corrupted account: held=%d balance=%d", a.Held, a.Balance)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_HoldExpiry_CaptureFails(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	base := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	h := authorize(t, "a", "alice", "bob", 300, time.Minute, base)
	later := base.Add(2 * time.Minute)
	if _, err := testStore.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300}, later); !errors.Is(err, ledger.ErrHoldExpired) {
		t.Errorf("capture err = %v, want ErrHoldExpired", err)
	}
	if a := getAccount(t, "alice"); a.Held != 0 || a.Available() != 1000 {
		t.Errorf("expired hold not released: held=%d", a.Held)
	}
	if hold, _, _ := testStore.GetHold(ctx, h.ID); hold.Status != ledger.HoldExpired {
		t.Errorf("hold status = %q, want expired", hold.Status)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

func TestPG_ExpireHolds_Sweep(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	base := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	authorize(t, "h1", "alice", "bob", 100, time.Minute, base)
	authorize(t, "h2", "alice", "bob", 150, time.Minute, base)
	authorize(t, "h3", "alice", "bob", 50, 0, base) // no expiry; must survive
	if a := getAccount(t, "alice"); a.Held != 300 {
		t.Fatalf("held = %d, want 300", a.Held)
	}

	n, err := testStore.ExpireHolds(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expired %d, want 2", n)
	}
	if a := getAccount(t, "alice"); a.Held != 50 {
		t.Errorf("held after sweep = %d, want 50 (the no-expiry hold)", a.Held)
	}
	// Re-running the sweep expires nothing more.
	if n, _ := testStore.ExpireHolds(ctx, base.Add(2*time.Minute)); n != 0 {
		t.Errorf("second sweep expired %d, want 0", n)
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}

// TestPG_Transfer_CannotSpendHeldFunds proves a direct transfer cannot move funds
// that an active hold has reserved, but can move what remains available.
func TestPG_Transfer_CannotSpendHeldFunds(t *testing.T) {
	reset(t)
	createAccount(t, "alice", "USD", 1000)
	createAccount(t, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	authorize(t, "hold", "alice", "bob", 800, 0, now) // available now 200

	if _, err := transfer(t, "t1", "alice", "bob", 500); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("transfer of held funds err = %v, want ErrInsufficientFunds", err)
	}
	if _, err := transfer(t, "t2", "alice", "bob", 200); err != nil {
		t.Errorf("transfer within available failed: %v", err)
	}
	a := getAccount(t, "alice")
	if a.Balance != 800 || a.Held != 800 || a.Available() != 0 {
		t.Errorf("alice balance=%d held=%d available=%d, want 800/800/0", a.Balance, a.Held, a.Available())
	}
	assertInvariants(t, map[string]ledger.Money{"alice": 1000, "bob": 0})
}
