package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mkmbhs/ledger"
)

const transferCols = `id, idempotency_key, from_account_id, to_account_id, amount, currency, status, created_at`

// ApplyPosting atomically and idempotently applies a balanced multi-leg
// posting: every account row is locked (in sorted order), every debited leg is
// funds-checked against its available balance, and the transfer, its entries,
// the balance changes, and the outbox event commit together or not at all. A
// two-party transfer is simply the two-leg case.
func (s *Store) ApplyPosting(ctx context.Context, req ledger.PostRequest) (ledger.Transfer, error) {
	var result ledger.Transfer
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Lock every posted account FIRST (in a consistent order), THEN check
		// idempotency. Locking first is what makes a concurrent same-key retry
		// correct: it serializes behind the original transaction and, once it
		// holds the locks, sees the committed transfer in the pre-check below —
		// returning the original result instead of re-running the funds check,
		// which could otherwise spuriously fail after the original already
		// debited the sources.
		ids := make([]string, len(req.Postings))
		for i, p := range req.Postings {
			ids[i] = p.AccountID
		}
		accounts, err := lockAccounts(ctx, tx, ids)
		if err != nil {
			return err
		}
		if existing, ok, err := readTransferByKey(ctx, tx, req.IdempotencyKey); err != nil {
			return err
		} else if ok {
			if !ledger.MatchesPost(existing, req) {
				return ledger.ErrIdempotencyConflict
			}
			result = existing
			return nil
		}

		// One shared currency: the declared req.Currency if given, otherwise the
		// accounts', which must all agree.
		currency := req.Currency
		for _, p := range req.Postings {
			a := accounts[p.AccountID]
			if currency == "" {
				currency = a.Currency
			}
			if a.Currency != currency {
				return ledger.ErrCurrencyMismatch
			}
		}
		// Every debited leg may only spend AVAILABLE funds: money fenced off by
		// an active hold cannot be moved out from under it.
		for _, p := range req.Postings {
			if p.Amount < 0 && accounts[p.AccountID].Available() < -p.Amount {
				return ledger.ErrInsufficientFunds
			}
		}

		t := ledger.NewPostedTransfer(req.IdempotencyKey, currency, req.Postings, time.Now().UTC(), newID)
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
		for _, p := range req.Postings {
			if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE id = $2`,
				int64(p.Amount), p.AccountID); err != nil {
				return err
			}
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
		// when the two requests post to disjoint account sets and thus share no
		// lock). Re-read the committed row and honor idempotency.
		existing, ok, rerr := readTransferByKey(ctx, s.pool, req.IdempotencyKey)
		if rerr != nil {
			return ledger.Transfer{}, rerr
		}
		if !ok {
			return ledger.Transfer{}, err
		}
		if !ledger.MatchesPost(existing, req) {
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

// insertTransferRows writes the transfer row and its entries. The two-leg
// summary columns (from_account_id, to_account_id, amount) are NULL for larger
// postings — the entries are the record. The UNIQUE constraint on
// transfers.idempotency_key turns a concurrent duplicate into a 23505 the
// caller maps to errDuplicate.
func insertTransferRows(ctx context.Context, q querier, t ledger.Transfer) error {
	var fromID, toID *string
	var amount *int64
	if t.FromAccountID != "" {
		a := int64(t.Amount)
		fromID, toID, amount = &t.FromAccountID, &t.ToAccountID, &a
	}
	if _, err := q.Exec(ctx, `
		INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		t.ID, t.IdempotencyKey, fromID, toID, amount, t.Currency, string(t.Status), t.CreatedAt); err != nil {
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

// loadEntries returns a transfer's entries, debits (negative) before credits,
// with id as the tiebreak so the order is deterministic.
func loadEntries(ctx context.Context, q querier, transferID string) ([]ledger.Entry, error) {
	rows, err := q.Query(ctx, `
		SELECT id, transfer_id, account_id, amount, created_at
		FROM entries WHERE transfer_id = $1
		ORDER BY amount, id`, transferID)
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
	var fromID, toID *string
	var amount *int64
	var status string
	if err := row.Scan(&t.ID, &t.IdempotencyKey, &fromID, &toID,
		&amount, &t.Currency, &status, &t.CreatedAt); err != nil {
		return ledger.Transfer{}, err
	}
	if fromID != nil {
		t.FromAccountID = *fromID
	}
	if toID != nil {
		t.ToAccountID = *toID
	}
	if amount != nil {
		t.Amount = ledger.Money(*amount)
	}
	t.Status = ledger.TransferStatus(status)
	return t, nil
}
