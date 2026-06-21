package ledger

import (
	"context"
	"fmt"
)

// Service holds the business rules of the ledger and delegates persistence and
// atomicity to a Store. Keeping validation here and atomicity in the Store keeps
// each layer small and testable.
type Service struct {
	store Store
}

// New returns a Service backed by the given Store.
func New(store Store) *Service {
	return &Service{store: store}
}

// Transfer validates the request and applies it through the Store. It is safe to
// retry with the same IdempotencyKey: the transfer is applied at most once.
func (s *Service) Transfer(ctx context.Context, req TransferRequest) (Transfer, error) {
	if req.Amount <= 0 {
		return Transfer{}, ErrInvalidAmount
	}
	if req.FromAccountID == req.ToAccountID {
		return Transfer{}, ErrSameAccount
	}
	if req.IdempotencyKey == "" {
		return Transfer{}, ErrMissingIdempotencyKey
	}
	return s.store.ApplyTransfer(ctx, req)
}

// AccountHistory returns every entry posted against an account, oldest first.
// The account's balance is always the sum of these entries.
func (s *Service) AccountHistory(ctx context.Context, accountID string) ([]Entry, error) {
	return s.store.AccountEntries(ctx, accountID)
}

// GetTransfer looks up a transfer by ID.
func (s *Service) GetTransfer(ctx context.Context, id string) (Transfer, bool, error) {
	return s.store.GetTransfer(ctx, id)
}

// Balance returns the current balance of an account.
func (s *Service) Balance(ctx context.Context, accountID string) (Money, error) {
	a, err := s.store.GetAccount(ctx, accountID)
	if err != nil {
		return 0, err
	}
	return a.Balance, nil
}

// CreateAccount registers an account with a starting balance.
func (s *Service) CreateAccount(ctx context.Context, id, currency string, opening Money) error {
	return s.store.CreateAccount(ctx, Account{ID: id, Currency: currency, Balance: opening})
}

// assertBalanced verifies the core double-entry invariant: a transfer's entries
// must sum to zero. It is a defensive check on an internal invariant; if it ever
// fires, there is a bug in the store.
func assertBalanced(entries []Entry) error {
	if len(entries) < 2 {
		return fmt.Errorf("ledger: a transfer needs at least two entries, got %d", len(entries))
	}
	var sum Money
	for _, e := range entries {
		sum += e.Amount
	}
	if sum != 0 {
		return fmt.Errorf("ledger: entries do not balance, sum=%d", sum)
	}
	return nil
}
