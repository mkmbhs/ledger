package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func newService(t *testing.T) *Service {
	t.Helper()
	return New(NewMemStore())
}

func mustAccount(t *testing.T, s *Service, id, ccy string, opening Money) {
	t.Helper()
	if err := s.CreateAccount(context.Background(), id, ccy, opening); err != nil {
		t.Fatalf("CreateAccount(%s): %v", id, err)
	}
}

func balance(t *testing.T, s *Service, id string) Money {
	t.Helper()
	b, err := s.Balance(context.Background(), id)
	if err != nil {
		t.Fatalf("Balance(%s): %v", id, err)
	}
	return b
}

func TestTransfer_Basic(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	tr, err := s.Transfer(context.Background(), TransferRequest{
		IdempotencyKey: "k1", FromAccountID: "alice", ToAccountID: "bob", Amount: 250,
	})
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	if got := balance(t, s, "alice"); got != 750 {
		t.Errorf("alice balance = %d, want 750", got)
	}
	if got := balance(t, s, "bob"); got != 250 {
		t.Errorf("bob balance = %d, want 250", got)
	}
	// Double-entry invariant: the two entries sum to zero.
	if err := AssertBalanced(tr.Entries); err != nil {
		t.Errorf("entries not balanced: %v", err)
	}
	if tr.Status != StatusPosted {
		t.Errorf("status = %q, want posted", tr.Status)
	}
}

func TestTransfer_Idempotent(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	req := TransferRequest{IdempotencyKey: "same", FromAccountID: "alice", ToAccountID: "bob", Amount: 100}
	first, err := s.Transfer(context.Background(), req)
	if err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	second, err := s.Transfer(context.Background(), req)
	if err != nil {
		t.Fatalf("second transfer: %v", err)
	}

	if first.ID != second.ID {
		t.Errorf("idempotent retry produced a different transfer: %s vs %s", first.ID, second.ID)
	}
	// The transfer must have been applied exactly once.
	if got := balance(t, s, "alice"); got != 900 {
		t.Errorf("alice balance = %d, want 900 (applied once)", got)
	}
	if got := balance(t, s, "bob"); got != 100 {
		t.Errorf("bob balance = %d, want 100 (applied once)", got)
	}
}

func TestTransfer_IdempotencyConflict(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	mustAccount(t, s, "carol", "USD", 0)

	if _, err := s.Transfer(context.Background(), TransferRequest{
		IdempotencyKey: "dup", FromAccountID: "alice", ToAccountID: "bob", Amount: 100,
	}); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Same key, different destination -> conflict.
	_, err := s.Transfer(context.Background(), TransferRequest{
		IdempotencyKey: "dup", FromAccountID: "alice", ToAccountID: "carol", Amount: 100,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestTransfer_Validation(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	mustAccount(t, s, "euro", "EUR", 0)

	cases := []struct {
		name string
		req  TransferRequest
		want error
	}{
		{"zero amount", TransferRequest{"k", "alice", "bob", 0}, ErrInvalidAmount},
		{"negative amount", TransferRequest{"k", "alice", "bob", -5}, ErrInvalidAmount},
		{"same account", TransferRequest{"k", "alice", "alice", 10}, ErrSameAccount},
		{"missing source", TransferRequest{"k", "ghost", "bob", 10}, ErrAccountNotFound},
		{"missing dest", TransferRequest{"k", "alice", "ghost", 10}, ErrAccountNotFound},
		{"currency mismatch", TransferRequest{"k", "alice", "euro", 10}, ErrCurrencyMismatch},
		{"insufficient funds", TransferRequest{"k", "alice", "bob", 100000}, ErrInsufficientFunds},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := s.Transfer(context.Background(), c.req)
			if !errors.Is(err, c.want) {
				t.Errorf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// TestTransfer_ConcurrentSameKey proves that many concurrent retries of the same
// idempotency key apply the transfer at most once. This is the property that
// keeps a payment system safe when clients retry on timeouts.
func TestTransfer_ConcurrentSameKey(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	const n = 200
	req := TransferRequest{IdempotencyKey: "retry", FromAccountID: "alice", ToAccountID: "bob", Amount: 100}

	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, err := s.Transfer(context.Background(), req)
			ids[i], errs[i] = tr.ID, err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d got a different transfer ID", i)
		}
	}
	if got := balance(t, s, "alice"); got != 900 {
		t.Errorf("alice balance = %d, want 900 (applied exactly once despite %d retries)", got, n)
	}
	if got := balance(t, s, "bob"); got != 100 {
		t.Errorf("bob balance = %d, want 100", got)
	}
}

// TestTransfer_ConcurrentConservation runs many distinct transfers concurrently
// and proves money is conserved: the sum of all balances is identical before and
// after. This catches lost updates and double-spends.
func TestTransfer_ConcurrentConservation(t *testing.T) {
	s := newService(t)
	const accounts = 10
	const opening Money = 1000
	for i := range accounts {
		mustAccount(t, s, fmt.Sprintf("acc-%d", i), "USD", opening)
	}
	total := func() Money {
		var sum Money
		for i := range accounts {
			sum += balance(t, s, fmt.Sprintf("acc-%d", i))
		}
		return sum
	}
	before := total()

	var wg sync.WaitGroup
	const rounds = 500
	for i := range rounds {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from := fmt.Sprintf("acc-%d", i%accounts)
			to := fmt.Sprintf("acc-%d", (i+1)%accounts)
			// Small amount that can always be covered; ignore the occasional
			// ErrInsufficientFunds which is a valid, money-conserving outcome.
			_, _ = s.Transfer(context.Background(), TransferRequest{
				IdempotencyKey: fmt.Sprintf("t-%d", i),
				FromAccountID:  from,
				ToAccountID:    to,
				Amount:         1,
			})
		}(i)
	}
	wg.Wait()

	if after := total(); after != before {
		t.Errorf("money not conserved: before=%d after=%d", before, after)
	}
}

// TestAssertBalanced exercises the exported double-entry check that every
// entry-writing path (in both stores) runs before committing.
func TestAssertBalanced(t *testing.T) {
	cases := []struct {
		name    string
		entries []Entry
		wantErr bool
	}{
		{"balanced pair", []Entry{{Amount: -100}, {Amount: 100}}, false},
		{"balanced multi", []Entry{{Amount: -100}, {Amount: 60}, {Amount: 40}}, false},
		{"unbalanced pair", []Entry{{Amount: -100}, {Amount: 99}}, true},
		{"both positive", []Entry{{Amount: 100}, {Amount: 100}}, true},
		{"single entry", []Entry{{Amount: 0}}, true},
		{"no entries", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := AssertBalanced(tc.entries); (err != nil) != tc.wantErr {
				t.Errorf("AssertBalanced(%v) err = %v, wantErr = %v", tc.entries, err, tc.wantErr)
			}
		})
	}
}

// TestCreateAccount_Idempotent pins the create semantics: an identical
// re-create is a no-op (a retried setup call is safe), while re-creating with
// different attributes is refused.
func TestCreateAccount_Idempotent(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)

	if err := s.CreateAccount(context.Background(), "alice", "USD", 1000); err != nil {
		t.Fatalf("identical re-create: %v", err)
	}
	if got := balance(t, s, "alice"); got != 1000 {
		t.Errorf("balance after identical re-create = %d, want 1000", got)
	}

	if err := s.CreateAccount(context.Background(), "alice", "EUR", 1000); !errors.Is(err, ErrAccountExists) {
		t.Errorf("re-create with different currency err = %v, want ErrAccountExists", err)
	}
	if err := s.CreateAccount(context.Background(), "alice", "USD", 5); !errors.Is(err, ErrAccountExists) {
		t.Errorf("re-create with different opening err = %v, want ErrAccountExists", err)
	}
}

// TestCreateAccount_DoesNotResetLiveBalance guards the footgun the semantics
// exist for: once money has moved, a re-create with the original opening no
// longer matches the live account and must be refused, not applied.
func TestCreateAccount_DoesNotResetLiveBalance(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	if _, err := s.Transfer(context.Background(), TransferRequest{
		IdempotencyKey: "k", FromAccountID: "alice", ToAccountID: "bob", Amount: 250,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateAccount(context.Background(), "alice", "USD", 1000); !errors.Is(err, ErrAccountExists) {
		t.Errorf("re-create of live account err = %v, want ErrAccountExists", err)
	}
	if got := balance(t, s, "alice"); got != 750 {
		t.Errorf("alice balance = %d, want 750 (unchanged)", got)
	}
	if got := balance(t, s, "bob"); got != 250 {
		t.Errorf("bob balance = %d, want 250 (unchanged)", got)
	}
}

// TestPost_Validation pins the Service-level shape checks for multi-leg
// postings: each violation gets its own sentinel and nothing reaches the store.
func TestPost_Validation(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	cases := []struct {
		name string
		req  PostRequest
		want error
	}{
		{"missing key", PostRequest{Postings: []Posting{{"alice", -1}, {"bob", 1}}}, ErrMissingIdempotencyKey},
		{"too few postings", PostRequest{IdempotencyKey: "k", Postings: []Posting{{"alice", -1}}}, ErrTooFewPostings},
		{"zero amount", PostRequest{IdempotencyKey: "k", Postings: []Posting{{"alice", 0}, {"bob", 0}}}, ErrZeroPosting},
		{"duplicate account", PostRequest{IdempotencyKey: "k", Postings: []Posting{{"alice", -1}, {"alice", 1}}}, ErrDuplicateAccount},
		{"unbalanced", PostRequest{IdempotencyKey: "k", Postings: []Posting{{"alice", -2}, {"bob", 1}}}, ErrUnbalancedPostings},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Post(ctx, tc.req); !errors.Is(err, tc.want) {
				t.Errorf("Post err = %v, want %v", err, tc.want)
			}
		})
	}
	if got := balance(t, s, "alice"); got != 1000 {
		t.Errorf("alice = %d, want 1000 (no rejected request may move money)", got)
	}
}

// TestPost_FeeSplit exercises the service path end to end: a three-leg split
// applies atomically and a replay of the same key applies exactly once.
func TestPost_FeeSplit(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "merchant", "USD", 0)
	mustAccount(t, s, "fees", "USD", 0)
	ctx := context.Background()

	req := PostRequest{
		IdempotencyKey: "split",
		Postings: []Posting{
			{AccountID: "alice", Amount: -100},
			{AccountID: "merchant", Amount: 97},
			{AccountID: "fees", Amount: 3},
		},
	}
	first, err := s.Post(ctx, req)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	second, err := s.Post(ctx, req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if first.ID != second.ID {
		t.Errorf("replay produced a different transfer")
	}
	if got := balance(t, s, "alice"); got != 900 {
		t.Errorf("alice = %d, want 900 (applied once)", got)
	}
	if got := balance(t, s, "merchant"); got != 97 {
		t.Errorf("merchant = %d, want 97", got)
	}
	if got := balance(t, s, "fees"); got != 3 {
		t.Errorf("fees = %d, want 3", got)
	}
	if err := AssertBalanced(first.Entries); err != nil {
		t.Errorf("entries: %v", err)
	}
}
