package ledger

import (
	"context"
	"time"
)

// Service holds the business rules of the ledger and delegates persistence and
// atomicity to a Store. Keeping validation here and atomicity in the Store keeps
// each layer small and testable. now is the clock (injectable in tests so hold
// expiry is deterministic).
type Service struct {
	store Store
	now   func() time.Time
}

// New returns a Service backed by the given Store.
func New(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

// Transfer validates the request and applies it as the two-leg special case of
// a posting: one debit and one matching credit. It is safe to retry with the
// same IdempotencyKey: the transfer is applied at most once.
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
	return s.store.ApplyPosting(ctx, PostRequest{
		IdempotencyKey: req.IdempotencyKey,
		Postings: []Posting{
			{AccountID: req.FromAccountID, Amount: -req.Amount},
			{AccountID: req.ToAccountID, Amount: req.Amount},
		},
	})
}

// Post validates a multi-leg posting and applies it through the Store: at
// least two legs, every amount non-zero, no account repeated, and the signed
// amounts summing to zero — money moves, it is never created. Safe to retry
// with the same IdempotencyKey: the posting is applied at most once.
func (s *Service) Post(ctx context.Context, req PostRequest) (Transfer, error) {
	if req.IdempotencyKey == "" {
		return Transfer{}, ErrMissingIdempotencyKey
	}
	if len(req.Postings) < 2 {
		return Transfer{}, ErrTooFewPostings
	}
	seen := make(map[string]bool, len(req.Postings))
	var sum Money
	for _, p := range req.Postings {
		if p.Amount == 0 {
			return Transfer{}, ErrZeroPosting
		}
		if seen[p.AccountID] {
			return Transfer{}, ErrDuplicateAccount
		}
		seen[p.AccountID] = true
		sum += p.Amount
	}
	if sum != 0 {
		return Transfer{}, ErrUnbalancedPostings
	}
	return s.store.ApplyPosting(ctx, req)
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

// Authorize reserves funds in the source account (an authorization hold). The
// money is fenced off (reducing Available) but not moved until the hold is
// captured.
func (s *Service) Authorize(ctx context.Context, req AuthorizeRequest) (Hold, error) {
	if req.Amount <= 0 {
		return Hold{}, ErrInvalidAmount
	}
	if req.FromAccountID == req.ToAccountID {
		return Hold{}, ErrSameAccount
	}
	if req.IdempotencyKey == "" {
		return Hold{}, ErrMissingIdempotencyKey
	}
	return s.store.Authorize(ctx, req, s.now())
}

// Capture settles all or part of a hold, moving the captured amount and
// releasing any remainder. Safe to retry with the same key.
func (s *Service) Capture(ctx context.Context, req CaptureRequest) (Transfer, error) {
	if req.Amount <= 0 {
		return Transfer{}, ErrInvalidAmount
	}
	if req.IdempotencyKey == "" {
		return Transfer{}, ErrMissingIdempotencyKey
	}
	return s.store.Capture(ctx, req, s.now())
}

// Void releases a hold without moving money. Idempotent.
func (s *Service) Void(ctx context.Context, holdID string) (Hold, error) {
	return s.store.Void(ctx, holdID)
}

// GetHold looks up a hold by ID.
func (s *Service) GetHold(ctx context.Context, id string) (Hold, bool, error) {
	return s.store.GetHold(ctx, id)
}

// ExpireHolds releases all holds whose deadline has passed, returning the count.
func (s *Service) ExpireHolds(ctx context.Context) (int, error) {
	return s.store.ExpireHolds(ctx, s.now())
}

// Balance returns the settled balance of an account.
func (s *Service) Balance(ctx context.Context, accountID string) (Money, error) {
	a, err := s.store.GetAccount(ctx, accountID)
	if err != nil {
		return 0, err
	}
	return a.Balance, nil
}

// Account returns the full account, including Held and Available, so a caller
// can see how much is reserved versus spendable.
func (s *Service) Account(ctx context.Context, id string) (Account, error) {
	return s.store.GetAccount(ctx, id)
}

// CreateAccount registers an account with a starting balance. Safe to retry:
// an identical re-create is a no-op, while re-creating an existing account with
// different attributes returns ErrAccountExists rather than resetting money.
func (s *Service) CreateAccount(ctx context.Context, id, currency string, opening Money) error {
	return s.store.CreateAccount(ctx, Account{ID: id, Currency: currency, Balance: opening})
}
