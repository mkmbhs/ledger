package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mkmbhs/ledger"
)

const holdCols = `id, idempotency_key, from_account_id, to_account_id, amount, captured, status, created_at, expires_at, capture_transfer_id`

// Authorize reserves req.Amount in the source account by raising its held figure
// (lowering Available) without moving money, and records an active hold. Atomic
// and idempotent on req.IdempotencyKey.
func (s *Store) Authorize(ctx context.Context, req ledger.AuthorizeRequest, now time.Time) (ledger.Hold, error) {
	var result ledger.Hold
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Lock the accounts first, then check idempotency under the lock — same
		// reasoning as ApplyPosting: a concurrent retry of the same key serializes
		// and reads back the original hold instead of double-reserving.
		accounts, err := lockAccounts(ctx, tx, []string{req.FromAccountID, req.ToAccountID})
		if err != nil {
			return err
		}
		from, to := accounts[req.FromAccountID], accounts[req.ToAccountID]
		if existing, ok, err := readHoldByKey(ctx, tx, req.IdempotencyKey); err != nil {
			return err
		} else if ok {
			if conflictsHold(existing, req) {
				return ledger.ErrIdempotencyConflict
			}
			result = existing
			return nil
		}

		if from.Currency != to.Currency {
			return ledger.ErrCurrencyMismatch
		}
		if from.Available() < req.Amount {
			return ledger.ErrInsufficientFunds
		}

		h := ledger.Hold{
			ID:             newID(),
			IdempotencyKey: req.IdempotencyKey,
			FromAccountID:  req.FromAccountID,
			ToAccountID:    req.ToAccountID,
			Amount:         req.Amount,
			Status:         ledger.HoldActive,
			CreatedAt:      now,
		}
		if req.ExpiresIn > 0 {
			h.ExpiresAt = now.Add(req.ExpiresIn)
		}
		if _, err := tx.Exec(ctx, `UPDATE accounts SET held = held + $1 WHERE id = $2`, int64(req.Amount), from.ID); err != nil {
			return err
		}
		if err := insertHold(ctx, tx, h); err != nil {
			if isUniqueViolation(err) {
				return errDuplicate
			}
			return err
		}
		result = h
		return nil
	})
	if errors.Is(err, errDuplicate) {
		existing, ok, rerr := readHoldByKey(ctx, s.pool, req.IdempotencyKey)
		if rerr != nil {
			return ledger.Hold{}, rerr
		}
		if !ok {
			return ledger.Hold{}, err
		}
		if conflictsHold(existing, req) {
			return ledger.Hold{}, ledger.ErrIdempotencyConflict
		}
		return existing, nil
	}
	if err != nil {
		return ledger.Hold{}, err
	}
	return result, nil
}

// Capture settles all or part of an active hold. It debits the source by the
// captured amount, releases the FULL reservation (so any uncaptured remainder
// returns to Available), credits the destination, records a transfer with two
// balanced entries, and marks the hold captured. Atomic and idempotent.
func (s *Store) Capture(ctx context.Context, req ledger.CaptureRequest, now time.Time) (ledger.Transfer, error) {
	// Fast path: a committed retry of the same capture key returns the original
	// transfer (the capture key lives in transfers.idempotency_key).
	if existing, ok, err := readTransferByKey(ctx, s.pool, req.IdempotencyKey); err != nil {
		return ledger.Transfer{}, err
	} else if ok {
		return existing, nil
	}

	var result ledger.Transfer
	var expired bool // a business outcome whose side effect (releasing the hold) must commit
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		h, err := lockHold(ctx, tx, req.HoldID)
		if err != nil {
			return err
		}
		// Re-check the capture key under the hold lock. A concurrent capture of the
		// same key serializes behind the original here and returns its committed
		// transfer, instead of erroring on the now-captured hold.
		if existing, ok, err := readTransferByKey(ctx, tx, req.IdempotencyKey); err != nil {
			return err
		} else if ok {
			result = existing
			return nil
		}

		if h.Status != ledger.HoldActive {
			return ledger.ErrHoldNotActive
		}
		if !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt) {
			// An expired hold cannot be captured; release the reservation and commit
			// that release, then report ErrHoldExpired out of band.
			if err := releaseHold(ctx, tx, h, ledger.HoldExpired); err != nil {
				return err
			}
			expired = true
			return nil
		}
		if req.Amount <= 0 {
			return ledger.ErrInvalidAmount
		}
		if req.Amount > h.Amount {
			return ledger.ErrCaptureExceedsHold
		}

		// Lock both accounts in the consistent order for the balance moves.
		accounts, err := lockAccounts(ctx, tx, []string{h.FromAccountID, h.ToAccountID})
		if err != nil {
			return err
		}
		from, to := accounts[h.FromAccountID], accounts[h.ToAccountID]
		// A capture settles as a two-leg posting: debit the source, credit the
		// destination. Captures stay two-leg by design (see README Limitations).
		t := ledger.NewPostedTransfer(req.IdempotencyKey, from.Currency, []ledger.Posting{
			{AccountID: h.FromAccountID, Amount: -req.Amount},
			{AccountID: h.ToAccountID, Amount: req.Amount},
		}, now, newID)
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
		// Debit the captured amount and release the whole reservation in one
		// statement (the remainder returns to Available); credit the destination.
		if _, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance - $1, held = held - $2 WHERE id = $3`,
			int64(req.Amount), int64(h.Amount), from.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance + $1 WHERE id = $2`,
			int64(req.Amount), to.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE holds SET status = $1, captured = $2, capture_transfer_id = $3 WHERE id = $4`,
			string(ledger.HoldCaptured), int64(req.Amount), t.ID, h.ID); err != nil {
			return err
		}
		// Emit the domain event in THIS transaction, so it commits atomically with
		// the capture's balance moves (transactional outbox — no dual-write window).
		if err := insertTransferPosted(ctx, tx, t); err != nil {
			return err
		}
		result = t
		return nil
	})
	if errors.Is(err, errDuplicate) {
		existing, ok, rerr := readTransferByKey(ctx, s.pool, req.IdempotencyKey)
		if rerr != nil {
			return ledger.Transfer{}, rerr
		}
		if !ok {
			return ledger.Transfer{}, err
		}
		return existing, nil
	}
	if err != nil {
		return ledger.Transfer{}, err
	}
	if expired {
		return ledger.Transfer{}, ledger.ErrHoldExpired
	}
	return result, nil
}

// Void releases an active hold without moving money. Voiding an already-voided
// hold is a no-op (idempotent) and must not double-release the reservation.
func (s *Store) Void(ctx context.Context, holdID string) (ledger.Hold, error) {
	var result ledger.Hold
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		h, err := lockHold(ctx, tx, holdID)
		if err != nil {
			return err
		}
		if h.Status == ledger.HoldVoided {
			result = h // already released; do nothing
			return nil
		}
		if h.Status != ledger.HoldActive {
			return ledger.ErrHoldNotActive
		}
		if err := releaseHold(ctx, tx, h, ledger.HoldVoided); err != nil {
			return err
		}
		h.Status = ledger.HoldVoided
		result = h
		return nil
	})
	if err != nil {
		return ledger.Hold{}, err
	}
	return result, nil
}

// GetHold returns a hold by id, or false if it does not exist.
func (s *Store) GetHold(ctx context.Context, id string) (ledger.Hold, bool, error) {
	h, err := scanHold(s.pool.QueryRow(ctx, `SELECT `+holdCols+` FROM holds WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Hold{}, false, nil
	}
	if err != nil {
		return ledger.Hold{}, false, err
	}
	return h, true, nil
}

// ExpireHolds releases every active hold past its deadline and marks it expired,
// returning how many were swept. The whole sweep is one atomic statement: the
// FOR UPDATE locks the doomed holds, their reservations are returned to the
// owning accounts in aggregate, and the holds are marked — all against one
// snapshot, so a concurrent capture either runs fully before or after the sweep.
func (s *Store) ExpireHolds(ctx context.Context, now time.Time) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		WITH expired AS (
			SELECT id, from_account_id, amount
			FROM holds
			WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at <= $1
			FOR UPDATE
		),
		released AS (
			UPDATE accounts a
			SET held = a.held - agg.total
			FROM (
				SELECT from_account_id, SUM(amount) AS total
				FROM expired GROUP BY from_account_id
			) agg
			WHERE a.id = agg.from_account_id
		),
		marked AS (
			UPDATE holds SET status = 'expired'
			WHERE id IN (SELECT id FROM expired)
			RETURNING 1
		)
		SELECT count(*) FROM marked`, now).Scan(&count)
	return count, err
}

// releaseHold returns a hold's reserved funds to its source account's available
// balance and marks the hold with a terminal status. The caller holds the hold's
// row lock; the UPDATE acquires the account row lock.
func releaseHold(ctx context.Context, q querier, h ledger.Hold, status ledger.HoldStatus) error {
	if _, err := q.Exec(ctx, `UPDATE accounts SET held = held - $1 WHERE id = $2`, int64(h.Amount), h.FromAccountID); err != nil {
		return err
	}
	if _, err := q.Exec(ctx, `UPDATE holds SET status = $1 WHERE id = $2`, string(status), h.ID); err != nil {
		return err
	}
	return nil
}

// lockHold locks one hold row FOR UPDATE, translating no-rows into ErrHoldNotFound.
func lockHold(ctx context.Context, tx pgx.Tx, id string) (ledger.Hold, error) {
	h, err := scanHold(tx.QueryRow(ctx, `SELECT `+holdCols+` FROM holds WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Hold{}, ledger.ErrHoldNotFound
	}
	return h, err
}

// readHoldByKey returns the hold bearing the given idempotency key, or ok=false.
func readHoldByKey(ctx context.Context, q querier, key string) (ledger.Hold, bool, error) {
	h, err := scanHold(q.QueryRow(ctx, `SELECT `+holdCols+` FROM holds WHERE idempotency_key = $1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Hold{}, false, nil
	}
	if err != nil {
		return ledger.Hold{}, false, err
	}
	return h, true, nil
}

func insertHold(ctx context.Context, q querier, h ledger.Hold) error {
	var expiresAt *time.Time
	if !h.ExpiresAt.IsZero() {
		e := h.ExpiresAt
		expiresAt = &e
	}
	_, err := q.Exec(ctx, `
		INSERT INTO holds (id, idempotency_key, from_account_id, to_account_id, amount, captured, status, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		h.ID, h.IdempotencyKey, h.FromAccountID, h.ToAccountID,
		int64(h.Amount), int64(h.Captured), string(h.Status), h.CreatedAt, expiresAt)
	return err
}

func scanHold(row pgx.Row) (ledger.Hold, error) {
	var h ledger.Hold
	var amount, captured int64
	var status string
	var expiresAt *time.Time
	var captureTransferID *string
	if err := row.Scan(&h.ID, &h.IdempotencyKey, &h.FromAccountID, &h.ToAccountID,
		&amount, &captured, &status, &h.CreatedAt, &expiresAt, &captureTransferID); err != nil {
		return ledger.Hold{}, err
	}
	h.Amount = ledger.Money(amount)
	h.Captured = ledger.Money(captured)
	h.Status = ledger.HoldStatus(status)
	if expiresAt != nil {
		h.ExpiresAt = *expiresAt
	}
	if captureTransferID != nil {
		h.CaptureTransferID = *captureTransferID
	}
	return h, nil
}

// conflictsHold reports whether an existing hold with a reused key was created
// with different parameters.
func conflictsHold(existing ledger.Hold, req ledger.AuthorizeRequest) bool {
	return existing.FromAccountID != req.FromAccountID ||
		existing.ToAccountID != req.ToAccountID ||
		existing.Amount != req.Amount
}
