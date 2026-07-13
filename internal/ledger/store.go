package ledger

import (
	"context"
	"time"
)

// Store is the persistence boundary for the ledger. Implementations own the
// transaction and concurrency guarantees: ApplyTransfer must be atomic (all of
// the balance changes, entries, and the transfer record commit together or not
// at all) and idempotent (a repeated IdempotencyKey returns the original
// transfer without applying it a second time).
//
// Two implementations exist:
//   - MemStore: an in-memory reference implementation used by the unit tests,
//     correct under concurrency via a single mutex.
//   - (M2) a PostgreSQL implementation using SELECT ... FOR UPDATE and a
//     unique constraint on the idempotency key inside a single SQL transaction.
type Store interface {
	// CreateAccount registers an account. Used for setup. Idempotent:
	// re-creating an account with identical attributes is a no-op; re-creating
	// with different attributes (including a balance that has since moved)
	// returns ErrAccountExists, so an existing balance is never silently reset.
	CreateAccount(ctx context.Context, a Account) error

	// GetAccount returns an account by ID, or ErrAccountNotFound.
	GetAccount(ctx context.Context, id string) (Account, error)

	// ApplyTransfer atomically and idempotently applies req. Validation of the
	// business rules (positive amount, distinct accounts) has already happened
	// in the Service; the Store enforces atomicity, currency match, sufficient
	// funds, and idempotency.
	ApplyTransfer(ctx context.Context, req TransferRequest) (Transfer, error)

	// GetTransfer returns a transfer by ID, or false if it does not exist.
	GetTransfer(ctx context.Context, id string) (Transfer, bool, error)

	// AccountEntries returns every entry posted against an account, oldest
	// first. This is how a statement or balance reconciliation is built: an
	// account's balance always equals the sum of its entries.
	AccountEntries(ctx context.Context, accountID string) ([]Entry, error)

	// Authorize reserves funds in the source account (increases its Held) without
	// moving money, and records a Hold. Atomic and idempotent on the key.
	Authorize(ctx context.Context, req AuthorizeRequest, now time.Time) (Hold, error)

	// Capture settles all or part of an active hold: it moves the captured amount
	// (producing balanced entries) and releases the rest of the reservation.
	// Atomic and idempotent on the key.
	Capture(ctx context.Context, req CaptureRequest, now time.Time) (Transfer, error)

	// Void releases an active hold without moving money. Idempotent: voiding an
	// already-voided hold is a no-op.
	Void(ctx context.Context, holdID string) (Hold, error)

	// GetHold returns a hold by ID, or false if it does not exist.
	GetHold(ctx context.Context, id string) (Hold, bool, error)

	// ExpireHolds releases every active hold whose ExpiresAt is at or before now,
	// returning how many were expired. This is the sweep an operator runs so
	// stale reservations do not fence off funds forever.
	ExpireHolds(ctx context.Context, now time.Time) (int, error)
}
