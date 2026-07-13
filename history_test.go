package ledger

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestAccountHistory(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	for i, amt := range []Money{100, 50, 25} {
		if _, err := s.Transfer(ctx, TransferRequest{
			IdempotencyKey: fmt.Sprintf("h%d", i), FromAccountID: "alice", ToAccountID: "bob", Amount: amt,
		}); err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}

	hist, err := s.AccountHistory(ctx, "alice")
	if err != nil {
		t.Fatalf("AccountHistory: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("alice history len = %d, want 3", len(hist))
	}

	// Reconciliation: opening + sum(entries) == current balance.
	var sum Money
	for _, e := range hist {
		if e.Amount >= 0 {
			t.Errorf("alice entry should be a debit (negative), got %d", e.Amount)
		}
		sum += e.Amount
	}
	if got := balance(t, s, "alice"); 1000+sum != got {
		t.Errorf("history does not reconcile: opening(1000)+entries(%d) != balance(%d)", sum, got)
	}

	// Returned slice is a copy: mutating it must not affect the store.
	hist[0].Amount = 99999
	if again, _ := s.AccountHistory(ctx, "alice"); again[0].Amount == 99999 {
		t.Error("AccountHistory leaked an internal slice; callers can corrupt the store")
	}

	if _, err := s.AccountHistory(ctx, "ghost"); !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("AccountHistory(ghost) err = %v, want ErrAccountNotFound", err)
	}
}

func TestMissingIdempotencyKey(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	_, err := s.Transfer(context.Background(), TransferRequest{
		FromAccountID: "alice", ToAccountID: "bob", Amount: 10, // no key
	})
	if !errors.Is(err, ErrMissingIdempotencyKey) {
		t.Errorf("err = %v, want ErrMissingIdempotencyKey", err)
	}
}

func TestGetTransfer(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	tr, err := s.Transfer(ctx, TransferRequest{IdempotencyKey: "g1", FromAccountID: "alice", ToAccountID: "bob", Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetTransfer(ctx, tr.ID)
	if err != nil || !ok {
		t.Fatalf("GetTransfer: ok=%v err=%v", ok, err)
	}
	if got.ID != tr.ID || got.Amount != 100 {
		t.Errorf("GetTransfer returned %+v, want id=%s amount=100", got, tr.ID)
	}
	if _, ok, _ := s.GetTransfer(ctx, "nope"); ok {
		t.Error("GetTransfer(nope) returned ok=true")
	}
}
