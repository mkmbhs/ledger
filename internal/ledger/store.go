package ledger

import "context"

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
	// CreateAccount registers an account. Used for setup.
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
}
