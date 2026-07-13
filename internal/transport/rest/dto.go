package rest

import (
	"time"

	"github.com/mkmbhs/ledger"
)

// The DTO types below define the public JSON shape of the API. They are kept
// separate from the ledger domain types so the wire format (snake_case, int64
// minor units) can evolve independently of the internal model.

type transferDTO struct {
	ID             string     `json:"id"`
	IdempotencyKey string     `json:"idempotency_key"`
	FromAccountID  string     `json:"from_account_id"`
	ToAccountID    string     `json:"to_account_id"`
	Amount         int64      `json:"amount"`
	Currency       string     `json:"currency"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
	Entries        []entryDTO `json:"entries"`
}

type entryDTO struct {
	ID         string    `json:"id"`
	TransferID string    `json:"transfer_id"`
	AccountID  string    `json:"account_id"`
	Amount     int64     `json:"amount"`
	CreatedAt  time.Time `json:"created_at"`
}

type holdDTO struct {
	ID                string     `json:"id"`
	IdempotencyKey    string     `json:"idempotency_key"`
	FromAccountID     string     `json:"from_account_id"`
	ToAccountID       string     `json:"to_account_id"`
	Amount            int64      `json:"amount"`
	Captured          int64      `json:"captured"`
	Status            string     `json:"status"`
	CreatedAt         time.Time  `json:"created_at"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	CaptureTransferID string     `json:"capture_transfer_id,omitempty"`
}

type accountDTO struct {
	ID        string `json:"id"`
	Currency  string `json:"currency"`
	Balance   int64  `json:"balance"`
	Held      int64  `json:"held"`
	Available int64  `json:"available"`
}

func toTransferDTO(t ledger.Transfer) transferDTO {
	entries := make([]entryDTO, len(t.Entries))
	for i, e := range t.Entries {
		entries[i] = toEntryDTO(e)
	}
	return transferDTO{
		ID:             t.ID,
		IdempotencyKey: t.IdempotencyKey,
		FromAccountID:  t.FromAccountID,
		ToAccountID:    t.ToAccountID,
		Amount:         int64(t.Amount),
		Currency:       t.Currency,
		Status:         string(t.Status),
		CreatedAt:      t.CreatedAt,
		Entries:        entries,
	}
}

func toEntryDTO(e ledger.Entry) entryDTO {
	return entryDTO{
		ID:         e.ID,
		TransferID: e.TransferID,
		AccountID:  e.AccountID,
		Amount:     int64(e.Amount),
		CreatedAt:  e.CreatedAt,
	}
}

func toHoldDTO(h ledger.Hold) holdDTO {
	d := holdDTO{
		ID:                h.ID,
		IdempotencyKey:    h.IdempotencyKey,
		FromAccountID:     h.FromAccountID,
		ToAccountID:       h.ToAccountID,
		Amount:            int64(h.Amount),
		Captured:          int64(h.Captured),
		Status:            string(h.Status),
		CreatedAt:         h.CreatedAt,
		CaptureTransferID: h.CaptureTransferID,
	}
	if !h.ExpiresAt.IsZero() {
		d.ExpiresAt = &h.ExpiresAt
	}
	return d
}

func toAccountDTO(a ledger.Account) accountDTO {
	return accountDTO{
		ID:        a.ID,
		Currency:  a.Currency,
		Balance:   int64(a.Balance),
		Held:      int64(a.Held),
		Available: int64(a.Available()),
	}
}
