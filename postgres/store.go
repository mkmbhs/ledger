// Package postgres is a PostgreSQL-backed implementation of ledger.Store.
//
// It provides the same two guarantees as the in-memory reference
// (ledger.MemStore), but durably and under real concurrency:
//
//   - Atomicity / no lost updates — every money-moving operation runs inside a
//     single pgx transaction that locks the involved account rows with
//     SELECT ... FOR UPDATE, always in a consistent order, before changing any
//     balance. Concurrent operations touching the same account serialize.
//   - Idempotency — enforced by the schema's UNIQUE constraints on
//     idempotency_key (transfers and holds). A retried request either reads the
//     original row back inside the locked transaction, or loses the insert race
//     and re-reads the committed row after catching the unique violation.
//
// All sentinel errors are the ledger package's own, so callers can keep using
// errors.Is across store implementations.
package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mkmbhs/ledger"
)

// Store is a PostgreSQL-backed ledger.Store.
type Store struct {
	pool *pgxpool.Pool
}

// compile-time proof that *Store satisfies the persistence boundary.
var _ ledger.Store = (*Store)(nil)

// New returns a Store backed by the given pgx pool. The caller owns the pool's
// lifecycle (and must Close it).
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Connect opens a new pool from a DSN and returns a ready Store together with the
// pool so the caller can Close it.
func Connect(ctx context.Context, dsn string) (*Store, *pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, err
	}
	return New(pool), pool, nil
}

// errDuplicate is an internal signal that an insert lost a race against a
// concurrent transaction holding the same idempotency key. The caller rolls back
// and re-reads the committed row. It never escapes the package.
var errDuplicate = errors.New("postgres: duplicate idempotency key")

// querier is the subset of the pgx API shared by *pgxpool.Pool and pgx.Tx, so
// helpers can run either against the pool (autocommit) or inside a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// inTx runs fn inside a transaction, committing on success and rolling back on
// any returned error. A business error that must keep its side effects (for
// example, releasing an expired hold) is reported out of band by the caller, not
// returned from fn.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	err = tx.Commit(ctx)
	return err
}

// isUniqueViolation reports whether err is a PostgreSQL unique-constraint
// violation (SQLSTATE 23505) — i.e. a collision on an idempotency_key.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// newID returns a random 128-bit hex id, matching the reference store.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// CreateAccount registers an account. Idempotent, mirroring the reference
// store: DO NOTHING on conflict, then read the existing row back and compare —
// an identical re-create is a no-op, anything else is ErrAccountExists. An
// existing account's money is never silently reset.
func (s *Store) CreateAccount(ctx context.Context, a ledger.Account) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO accounts (id, currency, balance, held)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING`,
		a.ID, a.Currency, int64(a.Balance), int64(a.Held))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	existing, err := s.GetAccount(ctx, a.ID)
	if err != nil {
		return err
	}
	if existing != a {
		return ledger.ErrAccountExists
	}
	return nil
}

// GetAccount returns an account by id, or ledger.ErrAccountNotFound.
func (s *Store) GetAccount(ctx context.Context, id string) (ledger.Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`SELECT id, currency, balance, held FROM accounts WHERE id = $1`, id))
}

// AccountEntries returns every entry posted against an account, oldest first.
// The account must exist (mirrors the reference store's ErrAccountNotFound).
func (s *Store) AccountEntries(ctx context.Context, accountID string) ([]ledger.Entry, error) {
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, transfer_id, account_id, amount, created_at
		FROM entries WHERE account_id = $1
		ORDER BY created_at, id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ledger.Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// scanAccount reads one account row, translating no-rows into ErrAccountNotFound.
func scanAccount(row pgx.Row) (ledger.Account, error) {
	var a ledger.Account
	var balance, held int64
	if err := row.Scan(&a.ID, &a.Currency, &balance, &held); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Account{}, ledger.ErrAccountNotFound
		}
		return ledger.Account{}, err
	}
	a.Balance = ledger.Money(balance)
	a.Held = ledger.Money(held)
	return a, nil
}

func scanEntry(row pgx.Row) (ledger.Entry, error) {
	var e ledger.Entry
	var amount int64
	if err := row.Scan(&e.ID, &e.TransferID, &e.AccountID, &amount, &e.CreatedAt); err != nil {
		return ledger.Entry{}, err
	}
	e.Amount = ledger.Money(amount)
	return e, nil
}

// lockAccount locks one account row FOR UPDATE.
func lockAccount(ctx context.Context, tx pgx.Tx, id string) (ledger.Account, error) {
	return scanAccount(tx.QueryRow(ctx,
		`SELECT id, currency, balance, held FROM accounts WHERE id = $1 FOR UPDATE`, id))
}

// lockAccounts locks every given account row FOR UPDATE in a deterministic
// order — lexicographically sorted ids — and returns the accounts keyed by id.
//
// The sorted order is the deadlock guard, and it generalizes unchanged from
// two accounts to n: any two concurrent postings whose account sets overlap
// (in any direction, with any leg count) acquire the locks they share in the
// same order, so one always serializes behind the other instead of each
// grabbing half and waiting forever for the rest.
func lockAccounts(ctx context.Context, tx pgx.Tx, ids []string) (map[string]ledger.Account, error) {
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)
	out := make(map[string]ledger.Account, len(sorted))
	for _, id := range sorted {
		if _, ok := out[id]; ok {
			continue // already locked (defensive; valid postings never repeat)
		}
		a, err := lockAccount(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		out[id] = a
	}
	return out, nil
}
