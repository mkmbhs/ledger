package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mkmbhs/ledger"
)

const transferCols = `id, idempotency_key, from_account_id, to_account_id, amount, currency, status, created_at`

// ApplyTransfer atomically and idempotently moves req.Amount from one account to
// another, recording the transfer and its two balanced entries.
func (s *Store) ApplyTransfer(ctx context.Context, req ledger.TransferRequest) (ledger.Transfer, error) {
	var result ledger.Transfer
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Lock both accounts FIRST (in a consistent order), THEN check idempotency.
		// Locking first is what makes a concurrent same-key retry correct: it
		// serializes behind the original transaction and, once it holds the locks,
		// sees the committed transfer in the pre-check below — returning the
		// original result instead of re-running the funds check, which could
		// otherwise spuriously fail after the original already debited the source.
		from, to, err := lockAccounts(ctx, tx, req.FromAccountID, req.ToAccountID)
		if err != nil {
			return err
		}
		if existing, ok, err := readTransferByKey(ctx, tx, req.IdempotencyKey); err != nil {
			return err
		} else if ok {
			if conflictsTransfer(existing, req) {
				return ledger.ErrIdempotencyConflict
			}
			result = existing
			return nil
		}

		if from.Currency != to.Currency {
			return ledger.ErrCurrencyMismatch
		}
		// A transfer may only spend AVAILABLE funds: money fenced off by an active
		// hold cannot be moved out from under it.
		if from.Available() < req.Amount {
			return ledger.ErrInsufficientFunds
		}

		t := buildTransfer(req.IdempotencyKey, from.ID, to.ID, from.Currency, req.Amount, time.Now().UTC())
		// The double-entry invariant is enforced on every path that writes
		// entries; a failure here rolls the transaction back untouched.
		if err := ledger.AssertBalanced(t.Entries); err != nil {
			return err
		}
		if err := insertTransferRows(ctx, tx, t); err != nil {
			if isUniqueViolation(err) {
				return errDuplicate
			}
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, int64(req.Amount), from.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE id = $2`, int64(req.Amount), to.ID); err != nil {
			return err
		}
		// Emit the domain event in THIS transaction, so it commits atomically with
		// the balance moves (transactional outbox — no dual-write window).
		if err := insertTransferPosted(ctx, tx, t); err != nil {
			return err
		}
		result = t
		return nil
	})
	if errors.Is(err, errDuplicate) {
		// Lost the insert race to a concurrent same-key transaction (only possible
		// when the two requests target different account pairs and thus share no
		// lock). Re-read the committed row and honor idempotency.
		existing, ok, rerr := readTransferByKey(ctx, s.pool, req.IdempotencyKey)
		if rerr != nil {
			return ledger.Transfer{}, rerr
		}
		if !ok {
			return ledger.Transfer{}, err
		}
		if conflictsTransfer(existing, req) {
			return ledger.Transfer{}, ledger.ErrIdempotencyConflict
		}
		return existing, nil
	}
	if err != nil {
		return ledger.Transfer{}, err
	}
	return result, nil
}

// GetTransfer returns a transfer (with its entries) by id, or false if absent.
func (s *Store) GetTransfer(ctx context.Context, id string) (ledger.Transfer, bool, error) {
	t, err := scanTransfer(s.pool.QueryRow(ctx,
		`SELECT `+transferCols+` FROM transfers WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transfer{}, false, nil
	}
	if err != nil {
		return ledger.Transfer{}, false, err
	}
	t.Entries, err = loadEntries(ctx, s.pool, t.ID)
	if err != nil {
		return ledger.Transfer{}, false, err
	}
	return t, true, nil
}

// buildTransfer constructs a posted transfer with its two balanced entries: a
// debit on the source and a matching credit on the destination, summing to zero.
func buildTransfer(key, fromID, toID, currency string, amount ledger.Money, createdAt time.Time) ledger.Transfer {
	tid := newID()
	return ledger.Transfer{
		ID:             tid,
		IdempotencyKey: key,
		FromAccountID:  fromID,
		ToAccountID:    toID,
		Amount:         amount,
		Currency:       currency,
		Status:         ledger.StatusPosted,
		CreatedAt:      createdAt,
		Entries: []ledger.Entry{
			{ID: newID(), TransferID: tid, AccountID: fromID, Amount: -amount, CreatedAt: createdAt},
			{ID: newID(), TransferID: tid, AccountID: toID, Amount: amount, CreatedAt: createdAt},
		},
	}
}

// insertTransferRows writes the transfer row and its entries. The UNIQUE
// constraint on transfers.idempotency_key turns a concurrent duplicate into a
// 23505 the caller maps to errDuplicate.
func insertTransferRows(ctx context.Context, q querier, t ledger.Transfer) error {
	if _, err := q.Exec(ctx, `
		INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		t.ID, t.IdempotencyKey, t.FromAccountID, t.ToAccountID, int64(t.Amount), t.Currency, string(t.Status), t.CreatedAt); err != nil {
		return err
	}
	for _, e := range t.Entries {
		if _, err := q.Exec(ctx, `
			INSERT INTO entries (id, transfer_id, account_id, amount, created_at)
			VALUES ($1, $2, $3, $4, $5)`,
			e.ID, e.TransferID, e.AccountID, int64(e.Amount), e.CreatedAt); err != nil {
			return err
		}
	}
	return nil
}

// readTransferByKey returns the transfer (with entries) bearing the given
// idempotency key, or ok=false if none exists yet.
func readTransferByKey(ctx context.Context, q querier, key string) (ledger.Transfer, bool, error) {
	t, err := scanTransfer(q.QueryRow(ctx,
		`SELECT `+transferCols+` FROM transfers WHERE idempotency_key = $1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transfer{}, false, nil
	}
	if err != nil {
		return ledger.Transfer{}, false, err
	}
	t.Entries, err = loadEntries(ctx, q, t.ID)
	if err != nil {
		return ledger.Transfer{}, false, err
	}
	return t, true, nil
}

// loadEntries returns a transfer's entries, debit (negative) before credit.
func loadEntries(ctx context.Context, q querier, transferID string) ([]ledger.Entry, error) {
	rows, err := q.Query(ctx, `
		SELECT id, transfer_id, account_id, amount, created_at
		FROM entries WHERE transfer_id = $1
		ORDER BY amount`, transferID)
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

func scanTransfer(row pgx.Row) (ledger.Transfer, error) {
	var t ledger.Transfer
	var amount int64
	var status string
	if err := row.Scan(&t.ID, &t.IdempotencyKey, &t.FromAccountID, &t.ToAccountID,
		&amount, &t.Currency, &status, &t.CreatedAt); err != nil {
		return ledger.Transfer{}, err
	}
	t.Amount = ledger.Money(amount)
	t.Status = ledger.TransferStatus(status)
	return t, nil
}

// conflictsTransfer reports whether an existing transfer with a reused key was
// created with different parameters — a client bug the reference store rejects.
func conflictsTransfer(existing ledger.Transfer, req ledger.TransferRequest) bool {
	return existing.FromAccountID != req.FromAccountID ||
		existing.ToAccountID != req.ToAccountID ||
		existing.Amount != req.Amount
}
