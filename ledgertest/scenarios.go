package ledgertest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mkmbhs/ledger"
)

// Scenarios returns the full conformance catalog, in a stable order. Each
// scenario receives its own fresh store from the Factory. The catalog is
// add-only while Version stays the same.
func Scenarios() []Scenario {
	return []Scenario{
		{"transfer/basic", scenarioTransferBasic},
		{"transfer/idempotent-replay", scenarioTransferIdempotentReplay},
		{"transfer/idempotency-conflict", scenarioTransferIdempotencyConflict},
		{"transfer/concurrent-same-key", scenarioTransferConcurrentSameKey},
		{"transfer/concurrent-conservation", scenarioTransferConcurrentConservation},
		{"transfer/insufficient-funds", scenarioTransferInsufficientFunds},
		{"transfer/unknown-account-and-currency-mismatch", scenarioTransferValidation},
		{"transfer/cannot-spend-held-funds", scenarioTransferCannotSpendHeldFunds},
		{"account/create-idempotent", scenarioCreateAccountIdempotent},
		{"account/entries-reconciliation", scenarioEntriesReconciliation},
		{"hold/authorize-reserves-without-moving", scenarioAuthorizeReservesWithoutMoving},
		{"hold/authorize-idempotent-and-conflict", scenarioAuthorizeIdempotentAndConflict},
		{"hold/authorize-insufficient-available", scenarioAuthorizeInsufficientAvailable},
		{"hold/capture-full", scenarioCaptureFull},
		{"hold/capture-partial-releases-remainder", scenarioCapturePartialReleasesRemainder},
		{"hold/capture-idempotent-concurrent", scenarioCaptureIdempotentConcurrent},
		{"hold/capture-errors", scenarioCaptureErrors},
		{"hold/void-releases-and-double-void", scenarioVoidReleasesAndDoubleVoid},
		{"hold/expired-capture-fails", scenarioHoldExpiryCaptureFails},
		{"hold/expire-sweep", scenarioExpireHoldsSweep},
	}
}

// AssertInvariants checks the ledger's safety properties for the given
// accounts (mapping account ID to its opening balance): no negative balance,
// held within balance, balance equals opening plus the sum of the account's
// entries (reconciliation), and the total across accounts equals the total of
// the openings (conservation). Implementations can call it after their own
// additional scenarios.
func AssertInvariants(t *testing.T, s ledger.Store, openings map[string]ledger.Money) {
	t.Helper()
	ctx := context.Background()
	var totalBalance, totalOpening ledger.Money
	for id, opening := range openings {
		a := getAccount(t, s, id)
		if a.Balance < 0 {
			t.Errorf("%s: negative balance %d", id, a.Balance)
		}
		if a.Held < 0 || a.Held > a.Balance {
			t.Errorf("%s: held=%d out of range for balance=%d", id, a.Held, a.Balance)
		}
		entries, err := s.AccountEntries(ctx, id)
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

// sc adapts a body that works against a ready store into a Scenario that
// obtains that store from the Factory.
func sc(body func(t *testing.T, s ledger.Store)) func(t *testing.T, f Factory) {
	return func(t *testing.T, f Factory) { body(t, f(t)) }
}

// --- helpers -----------------------------------------------------------------

func createAccount(t *testing.T, s ledger.Store, id, ccy string, opening ledger.Money) {
	t.Helper()
	if err := s.CreateAccount(context.Background(),
		ledger.Account{ID: id, Currency: ccy, Balance: opening}); err != nil {
		t.Fatalf("CreateAccount(%s): %v", id, err)
	}
}

func getAccount(t *testing.T, s ledger.Store, id string) ledger.Account {
	t.Helper()
	a, err := s.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatalf("GetAccount(%s): %v", id, err)
	}
	return a
}

func transfer(s ledger.Store, key, from, to string, amt ledger.Money) (ledger.Transfer, error) {
	return s.ApplyTransfer(context.Background(), ledger.TransferRequest{
		IdempotencyKey: key, FromAccountID: from, ToAccountID: to, Amount: amt,
	})
}

func authorize(t *testing.T, s ledger.Store, key, from, to string, amt ledger.Money, ttl time.Duration, now time.Time) ledger.Hold {
	t.Helper()
	h, err := s.Authorize(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: key, FromAccountID: from, ToAccountID: to, Amount: amt, ExpiresIn: ttl,
	}, now)
	if err != nil {
		t.Fatalf("Authorize(%s): %v", key, err)
	}
	return h
}

// --- transfer scenarios --------------------------------------------------------

// scenarioTransferBasic: a transfer moves money once, posts two balanced
// entries, and round-trips through GetTransfer.
var scenarioTransferBasic = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)

	tr, err := transfer(s, "k1", "alice", "bob", 250)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if tr.Status != ledger.StatusPosted {
		t.Errorf("status = %q, want posted", tr.Status)
	}
	if len(tr.Entries) != 2 || tr.Entries[0].Amount+tr.Entries[1].Amount != 0 {
		t.Errorf("entries not balanced: %+v", tr.Entries)
	}
	if a := getAccount(t, s, "alice"); a.Balance != 750 {
		t.Errorf("alice balance = %d, want 750", a.Balance)
	}
	if b := getAccount(t, s, "bob"); b.Balance != 250 {
		t.Errorf("bob balance = %d, want 250", b.Balance)
	}

	got, ok, err := s.GetTransfer(context.Background(), tr.ID)
	if err != nil || !ok {
		t.Fatalf("GetTransfer: ok=%v err=%v", ok, err)
	}
	if got.Amount != 250 || len(got.Entries) != 2 {
		t.Errorf("GetTransfer mismatch: %+v", got)
	}

	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioTransferIdempotentReplay: a repeated idempotency key returns the
// original transfer and the money moves exactly once.
var scenarioTransferIdempotentReplay = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)

	req := ledger.TransferRequest{IdempotencyKey: "same", FromAccountID: "alice", ToAccountID: "bob", Amount: 100}
	first, err := s.ApplyTransfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.ApplyTransfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Errorf("idempotent retry produced a different transfer: %s vs %s", first.ID, second.ID)
	}
	if a := getAccount(t, s, "alice"); a.Balance != 900 {
		t.Errorf("alice = %d, want 900 (applied once)", a.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioTransferIdempotencyConflict: reusing a key with different parameters
// is a client bug and is rejected without moving money.
var scenarioTransferIdempotencyConflict = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	createAccount(t, s, "carol", "USD", 0)

	if _, err := transfer(s, "dup", "alice", "bob", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := transfer(s, "dup", "alice", "carol", 100); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Errorf("err = %v, want ErrIdempotencyConflict", err)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0, "carol": 0})
})

// scenarioTransferConcurrentSameKey: many concurrent retries of one key apply
// the transfer exactly once and all observe the same result.
var scenarioTransferConcurrentSameKey = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)

	const n = 64
	req := ledger.TransferRequest{IdempotencyKey: "retry", FromAccountID: "alice", ToAccountID: "bob", Amount: 100}

	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, err := s.ApplyTransfer(context.Background(), req)
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
	if a := getAccount(t, s, "alice"); a.Balance != 900 {
		t.Errorf("alice = %d, want 900 (applied once despite %d retries)", a.Balance, n)
	}
	if b := getAccount(t, s, "bob"); b.Balance != 100 {
		t.Errorf("bob = %d, want 100", b.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioTransferConcurrentConservation: concurrent transfers around a ring of
// accounts (including opposite directions on the same pair) conserve the total
// and never deadlock or lose an update.
var scenarioTransferConcurrentConservation = sc(func(t *testing.T, s ledger.Store) {
	const accounts = 8
	const opening ledger.Money = 1000
	openings := make(map[string]ledger.Money, accounts)
	for i := range accounts {
		id := fmt.Sprintf("acc-%d", i)
		createAccount(t, s, id, "USD", opening)
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
			// ErrInsufficientFunds is a valid, money-conserving outcome.
			_, _ = transfer(s, fmt.Sprintf("c-%d", i), from, to, 1)
		}(i)
	}
	wg.Wait()

	AssertInvariants(t, s, openings)
})

// scenarioTransferInsufficientFunds: a transfer that exceeds the available
// balance is rejected and changes nothing.
var scenarioTransferInsufficientFunds = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 100)
	createAccount(t, s, "bob", "USD", 0)

	if _, err := transfer(s, "k", "alice", "bob", 500); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("err = %v, want ErrInsufficientFunds", err)
	}
	if a := getAccount(t, s, "alice"); a.Balance != 100 {
		t.Errorf("alice balance changed on failed transfer: %d", a.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 100, "bob": 0})
})

// scenarioTransferValidation: unknown accounts and currency mismatches are
// rejected with the ledger's sentinel errors.
var scenarioTransferValidation = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "euro", "EUR", 0)

	if _, err := transfer(s, "x", "alice", "ghost", 10); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Errorf("missing dest err = %v, want ErrAccountNotFound", err)
	}
	if _, err := transfer(s, "y", "alice", "euro", 10); !errors.Is(err, ledger.ErrCurrencyMismatch) {
		t.Errorf("currency err = %v, want ErrCurrencyMismatch", err)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "euro": 0})
})

// scenarioTransferCannotSpendHeldFunds: a direct transfer can spend only
// available funds — money reserved by an active hold is fenced off.
var scenarioTransferCannotSpendHeldFunds = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	authorize(t, s, "hold", "alice", "bob", 800, 0, now) // available now 200

	if _, err := transfer(s, "t1", "alice", "bob", 500); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("transfer of held funds err = %v, want ErrInsufficientFunds", err)
	}
	if _, err := transfer(s, "t2", "alice", "bob", 200); err != nil {
		t.Errorf("transfer within available failed: %v", err)
	}
	a := getAccount(t, s, "alice")
	if a.Balance != 800 || a.Held != 800 || a.Available() != 0 {
		t.Errorf("alice balance=%d held=%d available=%d, want 800/800/0", a.Balance, a.Held, a.Available())
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// --- account scenarios ----------------------------------------------------------

// scenarioCreateAccountIdempotent: an identical re-create is a no-op, a
// mismatched one returns ErrAccountExists, and a live balance is never reset.
var scenarioCreateAccountIdempotent = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	if err := s.CreateAccount(ctx, ledger.Account{ID: "alice", Currency: "USD", Balance: 1000}); err != nil {
		t.Fatalf("identical re-create: %v", err)
	}
	if err := s.CreateAccount(ctx, ledger.Account{ID: "alice", Currency: "EUR", Balance: 1000}); !errors.Is(err, ledger.ErrAccountExists) {
		t.Errorf("different currency err = %v, want ErrAccountExists", err)
	}

	if _, err := transfer(s, "k1", "alice", "bob", 250); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAccount(ctx, ledger.Account{ID: "alice", Currency: "USD", Balance: 1000}); !errors.Is(err, ledger.ErrAccountExists) {
		t.Errorf("re-create of live account err = %v, want ErrAccountExists", err)
	}
	if a := getAccount(t, s, "alice"); a.Balance != 750 {
		t.Errorf("alice balance = %d, want 750 (never reset)", a.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioEntriesReconciliation: an account's entries, oldest first, always sum
// to its balance delta; unknown accounts are ErrAccountNotFound.
var scenarioEntriesReconciliation = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)

	for i, amt := range []ledger.Money{100, 50, 25} {
		if _, err := transfer(s, fmt.Sprintf("h-%d", i), "alice", "bob", amt); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := s.AccountEntries(context.Background(), "alice")
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
	if _, err := s.AccountEntries(context.Background(), "ghost"); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Errorf("unknown account err = %v, want ErrAccountNotFound", err)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// --- hold scenarios -------------------------------------------------------------

// scenarioAuthorizeReservesWithoutMoving: a hold raises Held (lowering
// Available) without moving any money.
var scenarioAuthorizeReservesWithoutMoving = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, s, "a1", "alice", "bob", 300, 0, now)
	if h.Status != ledger.HoldActive {
		t.Errorf("status = %q, want active", h.Status)
	}
	a := getAccount(t, s, "alice")
	if a.Balance != 1000 || a.Held != 300 || a.Available() != 700 {
		t.Errorf("alice balance=%d held=%d available=%d, want 1000/300/700", a.Balance, a.Held, a.Available())
	}
	if b := getAccount(t, s, "bob"); b.Balance != 0 {
		t.Errorf("bob received money before capture: %d", b.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioAuthorizeIdempotentAndConflict: a replayed authorize returns the
// original hold (reserving once); a reused key with different parameters is
// rejected.
var scenarioAuthorizeIdempotentAndConflict = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h1 := authorize(t, s, "same", "alice", "bob", 300, 0, now)
	h2 := authorize(t, s, "same", "alice", "bob", 300, 0, now)
	if h1.ID != h2.ID {
		t.Errorf("idempotent authorize made two holds")
	}
	if a := getAccount(t, s, "alice"); a.Held != 300 {
		t.Errorf("held = %d, want 300 (reserved once)", a.Held)
	}
	if _, err := s.Authorize(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: "same", FromAccountID: "alice", ToAccountID: "bob", Amount: 999,
	}, now); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Errorf("err = %v, want ErrIdempotencyConflict", err)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioAuthorizeInsufficientAvailable: a hold can only reserve available
// funds — an existing hold fences off its share.
var scenarioAuthorizeInsufficientAvailable = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	authorize(t, s, "a1", "alice", "bob", 700, 0, now) // available now 300
	if _, err := s.Authorize(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: "a2", FromAccountID: "alice", ToAccountID: "bob", Amount: 400,
	}, now); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("err = %v, want ErrInsufficientFunds", err)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioCaptureFull: capturing the full hold moves the money, releases the
// reservation, and links the hold to its settlement transfer.
var scenarioCaptureFull = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, s, "a", "alice", "bob", 300, 0, now)
	tr, err := s.Capture(context.Background(), ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300}, now)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if tr.Amount != 300 {
		t.Errorf("transfer amount = %d, want 300", tr.Amount)
	}
	a := getAccount(t, s, "alice")
	if a.Balance != 700 || a.Held != 0 {
		t.Errorf("alice balance=%d held=%d, want 700/0", a.Balance, a.Held)
	}
	if b := getAccount(t, s, "bob"); b.Balance != 300 {
		t.Errorf("bob = %d, want 300", b.Balance)
	}
	if hold, _, _ := s.GetHold(context.Background(), h.ID); hold.Status != ledger.HoldCaptured || hold.Captured != 300 || hold.CaptureTransferID != tr.ID {
		t.Errorf("hold = %q/%d/%s, want captured/300/%s", hold.Status, hold.Captured, hold.CaptureTransferID, tr.ID)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioCapturePartialReleasesRemainder: a partial capture moves only the
// captured amount and returns the rest of the reservation to available.
var scenarioCapturePartialReleasesRemainder = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, s, "a", "alice", "bob", 300, 0, now)
	if _, err := s.Capture(context.Background(), ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 100}, now); err != nil {
		t.Fatal(err)
	}
	a := getAccount(t, s, "alice")
	if a.Balance != 900 || a.Held != 0 || a.Available() != 900 {
		t.Errorf("alice balance=%d held=%d available=%d, want 900/0/900", a.Balance, a.Held, a.Available())
	}
	if b := getAccount(t, s, "bob"); b.Balance != 100 {
		t.Errorf("bob = %d, want 100", b.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioCaptureIdempotentConcurrent: many concurrent captures of one key
// settle the hold exactly once and all observe the same transfer.
var scenarioCaptureIdempotentConcurrent = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()

	h := authorize(t, s, "a", "alice", "bob", 300, 0, now)
	req := ledger.CaptureRequest{IdempotencyKey: "cap", HoldID: h.ID, Amount: 300}

	const n = 32
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, err := s.Capture(context.Background(), req, now)
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
	if b := getAccount(t, s, "bob"); b.Balance != 300 {
		t.Errorf("bob = %d, want 300 (captured exactly once)", b.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioCaptureErrors: over-capture, capture of an unknown hold, and capture
// after void are each rejected with their sentinel errors.
var scenarioCaptureErrors = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300, 0, now)
	if _, err := s.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "x", HoldID: h.ID, Amount: 400}, now); !errors.Is(err, ledger.ErrCaptureExceedsHold) {
		t.Errorf("over-capture err = %v, want ErrCaptureExceedsHold", err)
	}
	if _, err := s.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "y", HoldID: "ghost", Amount: 10}, now); !errors.Is(err, ledger.ErrHoldNotFound) {
		t.Errorf("unknown hold err = %v, want ErrHoldNotFound", err)
	}
	if _, err := s.Void(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "z", HoldID: h.ID, Amount: 100}, now); !errors.Is(err, ledger.ErrHoldNotActive) {
		t.Errorf("capture-after-void err = %v, want ErrHoldNotActive", err)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioVoidReleasesAndDoubleVoid: void releases the reservation without
// moving money, and a second void is a no-op that never double-releases.
var scenarioVoidReleasesAndDoubleVoid = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	now := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300, 0, now)
	vh, err := s.Void(ctx, h.ID)
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if vh.Status != ledger.HoldVoided {
		t.Errorf("status = %q, want voided", vh.Status)
	}
	if a := getAccount(t, s, "alice"); a.Held != 0 || a.Available() != 1000 {
		t.Errorf("alice not fully released: held=%d", a.Held)
	}
	if _, err := s.Void(ctx, h.ID); err != nil {
		t.Fatalf("second Void: %v", err)
	}
	if a := getAccount(t, s, "alice"); a.Held != 0 || a.Balance != 1000 {
		t.Errorf("double Void corrupted account: held=%d balance=%d", a.Held, a.Balance)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioHoldExpiryCaptureFails: an expired hold cannot be captured; the
// attempt releases the reservation and marks the hold expired.
var scenarioHoldExpiryCaptureFails = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	base := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300, time.Minute, base)
	later := base.Add(2 * time.Minute)
	if _, err := s.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300}, later); !errors.Is(err, ledger.ErrHoldExpired) {
		t.Errorf("capture err = %v, want ErrHoldExpired", err)
	}
	if a := getAccount(t, s, "alice"); a.Held != 0 || a.Available() != 1000 {
		t.Errorf("expired hold not released: held=%d", a.Held)
	}
	if hold, _, _ := s.GetHold(ctx, h.ID); hold.Status != ledger.HoldExpired {
		t.Errorf("hold status = %q, want expired", hold.Status)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})

// scenarioExpireHoldsSweep: the expiry sweep releases exactly the overdue
// holds, leaves open-ended ones alone, and is idempotent.
var scenarioExpireHoldsSweep = sc(func(t *testing.T, s ledger.Store) {
	createAccount(t, s, "alice", "USD", 1000)
	createAccount(t, s, "bob", "USD", 0)
	base := time.Unix(1_700_000_000, 0).UTC()
	ctx := context.Background()

	authorize(t, s, "h1", "alice", "bob", 100, time.Minute, base)
	authorize(t, s, "h2", "alice", "bob", 150, time.Minute, base)
	authorize(t, s, "h3", "alice", "bob", 50, 0, base) // no expiry; must survive
	if a := getAccount(t, s, "alice"); a.Held != 300 {
		t.Fatalf("held = %d, want 300", a.Held)
	}

	n, err := s.ExpireHolds(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expired %d, want 2", n)
	}
	if a := getAccount(t, s, "alice"); a.Held != 50 {
		t.Errorf("held after sweep = %d, want 50 (the no-expiry hold)", a.Held)
	}
	if n, _ := s.ExpireHolds(ctx, base.Add(2*time.Minute)); n != 0 {
		t.Errorf("second sweep expired %d, want 0", n)
	}
	AssertInvariants(t, s, map[string]ledger.Money{"alice": 1000, "bob": 0})
})
