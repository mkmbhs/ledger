// Package ledger implements a double-entry ledger with idempotent money movement.
//
// Design in one line: every transfer produces two balanced entries (a debit and
// a credit that sum to zero), money is never created or destroyed, and applying
// the same request twice (same idempotency key) has the same effect as applying
// it once.
package ledger

import (
	"errors"
	"time"
)

// Money is an amount in an account's minor units (for example, cents). Integers
// are used deliberately: never represent money as a float.
type Money int64

// Account is a holder of a balance in a single currency.
type Account struct {
	ID       string
	Currency string
	Balance  Money
}

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

// Errors returned by the ledger. Callers can match on these with errors.Is.
var (
	ErrInvalidAmount         = errors.New("ledger: amount must be positive")
	ErrSameAccount           = errors.New("ledger: source and destination must differ")
	ErrMissingIdempotencyKey = errors.New("ledger: idempotency key is required")
	ErrAccountNotFound       = errors.New("ledger: account not found")
	ErrCurrencyMismatch      = errors.New("ledger: account currencies do not match")
	ErrInsufficientFunds     = errors.New("ledger: insufficient funds")
	ErrIdempotencyConflict   = errors.New("ledger: idempotency key reused with different parameters")
)
