// Package ledger implements a double-entry ledger with idempotent money movement.
//
// Design in one line: every transfer produces two balanced entries (a debit and
// a credit that sum to zero), money is never created or destroyed, and applying
// the same request twice (same idempotency key) has the same effect as applying
// it once.
package ledger

import (
	"errors"
	"fmt"
	"time"
)

// Money is an amount in an account's minor units (for example, cents). Integers
// are used deliberately: never represent money as a float.
type Money int64

// Account is a holder of a balance in a single currency.
//
// Two figures matter for a wallet:
//   - Balance is the settled money the account owns.
//   - Held is the portion reserved by active authorization holds.
//
// Available (Balance - Held) is what can actually be spent or newly held. This
// is the model behind a card authorization, a hotel pre-auth, or reserving a
// wager: the money is fenced off before it moves.
type Account struct {
	ID       string
	Currency string
	Balance  Money
	Held     Money
}

// Available returns the spendable balance: settled funds minus active holds.
func (a Account) Available() Money { return a.Balance - a.Held }

// Entry is one side of a transfer posted against a single account. Amount is
// signed: negative for a debit (money leaving), positive for a credit (money
// arriving). The two entries of a transfer always sum to zero.
type Entry struct {
	ID         string
	TransferID string
	AccountID  string
	Amount     Money
	CreatedAt  time.Time
}

// AssertBalanced verifies the core double-entry invariant: a transfer's entries
// must sum to zero, and there must be at least two of them. Every path that
// writes entries — in every Store implementation — must call this before
// committing any state; if it ever fires, there is a bug in the store, and the
// write is refused rather than let the books go out of balance.
func AssertBalanced(entries []Entry) error {
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

// TransferStatus is the lifecycle state of a transfer. In M1 a transfer is
// applied atomically, so it is created already Posted.
type TransferStatus string

const (
	StatusPosted TransferStatus = "posted"
)

// Transfer is an atomic, balanced movement of money from one account to another.
type Transfer struct {
	ID             string
	IdempotencyKey string
	FromAccountID  string
	ToAccountID    string
	Amount         Money
	Currency       string
	Status         TransferStatus
	CreatedAt      time.Time
	Entries        []Entry
}

// TransferRequest is the input to apply a transfer. IdempotencyKey makes the
// operation safe to retry: the first request with a given key applies the
// transfer; later requests with the same key return the original result without
// applying it again.
type TransferRequest struct {
	IdempotencyKey string
	FromAccountID  string
	ToAccountID    string
	Amount         Money
}

// HoldStatus is the lifecycle state of an authorization hold.
type HoldStatus string

const (
	HoldActive   HoldStatus = "active"   // funds reserved, not yet moved
	HoldCaptured HoldStatus = "captured" // settled: funds moved (fully or partially)
	HoldVoided   HoldStatus = "voided"   // released without moving funds
	HoldExpired  HoldStatus = "expired"  // released automatically after ExpiresAt
)

// Hold is an authorization: it reserves funds in the source account so they
// cannot be spent twice, before deciding whether to capture (move) or release
// them. A hold moves no money until it is captured. This is the primitive behind
// card auth-then-capture, wallet reserve-then-settle, and placing then settling
// a wager.
type Hold struct {
	ID                string
	IdempotencyKey    string
	FromAccountID     string
	ToAccountID       string
	Amount            Money // reserved amount
	Captured          Money // amount actually captured (<= Amount)
	Status            HoldStatus
	CreatedAt         time.Time
	ExpiresAt         time.Time // zero means no expiry
	CaptureTransferID string    // set once captured
}

// AuthorizeRequest reserves funds. ExpiresIn, if > 0, sets a deadline after
// which the hold is released automatically by ExpireHolds.
type AuthorizeRequest struct {
	IdempotencyKey string
	FromAccountID  string
	ToAccountID    string
	Amount         Money
	ExpiresIn      time.Duration
}

// CaptureRequest settles all or part of a hold. Amount must be > 0 and <= the
// hold's reserved amount; any uncaptured remainder is released.
type CaptureRequest struct {
	IdempotencyKey string
	HoldID         string
	Amount         Money
}

// Errors returned by the ledger. Callers can match on these with errors.Is.
var (
	ErrInvalidAmount         = errors.New("ledger: amount must be positive")
	ErrSameAccount           = errors.New("ledger: source and destination must differ")
	ErrMissingIdempotencyKey = errors.New("ledger: idempotency key is required")
	ErrAccountNotFound       = errors.New("ledger: account not found")
	ErrAccountExists         = errors.New("ledger: account already exists with different attributes")
	ErrCurrencyMismatch      = errors.New("ledger: account currencies do not match")
	ErrInsufficientFunds     = errors.New("ledger: insufficient funds")
	ErrIdempotencyConflict   = errors.New("ledger: idempotency key reused with different parameters")
	ErrHoldNotFound          = errors.New("ledger: hold not found")
	ErrHoldNotActive         = errors.New("ledger: hold is not active")
	ErrHoldExpired           = errors.New("ledger: hold has expired")
	ErrCaptureExceedsHold    = errors.New("ledger: capture amount exceeds the held amount")
)
