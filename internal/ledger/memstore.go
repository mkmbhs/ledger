package ledger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// MemStore is an in-memory, concurrency-safe reference implementation of Store.
// It exists to specify, in the simplest possible way, exactly what the
// PostgreSQL implementation must guarantee: atomic balanced application and
// idempotent retries. A single mutex makes every operation serializable, so the
// concurrency tests here are an executable specification of correctness.
type MemStore struct {
	mu               sync.Mutex
	accounts         map[string]Account
	transfers        map[string]Transfer
	byIdemKey        map[string]string  // transfer idempotency key -> transfer ID
	entriesByAccount map[string][]Entry // account ID -> its entries, oldest first
	holds            map[string]Hold    // hold ID -> hold
	holdByKey        map[string]string  // authorize idempotency key -> hold ID
	captureByKey     map[string]string  // capture idempotency key -> transfer ID
	now              func() time.Time
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		accounts:         make(map[string]Account),
		transfers:        make(map[string]Transfer),
		byIdemKey:        make(map[string]string),
		entriesByAccount: make(map[string][]Entry),
		holds:            make(map[string]Hold),
		holdByKey:        make(map[string]string),
		captureByKey:     make(map[string]string),
		now:              time.Now,
	}
}

// CreateAccount registers an account. Idempotent: re-creating an account with
// identical attributes is a no-op, so a retried setup call is safe. Re-creating
// with different attributes — including a balance that has since moved — returns
// ErrAccountExists: an existing account's money is never silently reset.
func (s *MemStore) CreateAccount(_ context.Context, a Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.accounts[a.ID]; ok {
		if existing != a {
			return ErrAccountExists
		}
		return nil
	}
	s.accounts[a.ID] = a
	return nil
}

func (s *MemStore) GetAccount(_ context.Context, id string) (Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[id]
	if !ok {
		return Account{}, ErrAccountNotFound
	}
	return a, nil
}

func (s *MemStore) GetTransfer(_ context.Context, id string) (Transfer, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.transfers[id]
	return t, ok, nil
}

// ApplyTransfer is the critical section. Holding the mutex for the whole
// operation makes it atomic and serializable: no lost updates, no double-spend,
// and idempotency is enforced without races.
func (s *MemStore) ApplyTransfer(_ context.Context, req TransferRequest) (Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotency: a repeated key returns the original transfer. The same key
	// with different parameters is a client bug and is rejected.
	if id, ok := s.byIdemKey[req.IdempotencyKey]; ok {
		existing := s.transfers[id]
		if existing.FromAccountID != req.FromAccountID ||
			existing.ToAccountID != req.ToAccountID ||
			existing.Amount != req.Amount {
			return Transfer{}, ErrIdempotencyConflict
		}
		return existing, nil
	}

	from, ok := s.accounts[req.FromAccountID]
	if !ok {
		return Transfer{}, ErrAccountNotFound
	}
	to, ok := s.accounts[req.ToAccountID]
	if !ok {
		return Transfer{}, ErrAccountNotFound
	}
	if from.Currency != to.Currency {
		return Transfer{}, ErrCurrencyMismatch
	}
	// A transfer may only spend AVAILABLE funds: money reserved by an active
	// hold cannot be moved out from under it.
	if from.Available() < req.Amount {
		return Transfer{}, ErrInsufficientFunds
	}

	// Build the transfer with its two balanced entries.
	now := s.now()
	tid := newID()
	t := Transfer{
		ID:             tid,
		IdempotencyKey: req.IdempotencyKey,
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		Amount:         req.Amount,
		Currency:       from.Currency,
		Status:         StatusPosted,
		CreatedAt:      now,
		Entries: []Entry{
			{ID: newID(), TransferID: tid, AccountID: from.ID, Amount: -req.Amount, CreatedAt: now},
			{ID: newID(), TransferID: tid, AccountID: to.ID, Amount: req.Amount, CreatedAt: now},
		},
	}

	// Invariant: the entries of a transfer always sum to zero (money is neither
	// created nor destroyed). Checked before any state changes so a failed write
	// leaves the store untouched. This is the heart of double-entry accounting.
	if err := AssertBalanced(t.Entries); err != nil {
		return Transfer{}, err
	}

	// Apply the balance changes.
	from.Balance -= req.Amount
	to.Balance += req.Amount
	s.accounts[from.ID] = from
	s.accounts[to.ID] = to

	s.transfers[tid] = t
	s.byIdemKey[req.IdempotencyKey] = tid
	for _, e := range t.Entries {
		s.entriesByAccount[e.AccountID] = append(s.entriesByAccount[e.AccountID], e)
	}
	return t, nil
}

// AccountEntries returns a copy of an account's entries, oldest first. A copy is
// returned so a caller cannot mutate the store's internal slice.
func (s *MemStore) AccountEntries(_ context.Context, accountID string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.accounts[accountID]; !ok {
		return nil, ErrAccountNotFound
	}
	src := s.entriesByAccount[accountID]
	out := make([]Entry, len(src))
	copy(out, src)
	return out, nil
}

// Authorize reserves req.Amount in the source account: it raises Held (lowering
// Available) without moving any money, and records an active Hold.
func (s *MemStore) Authorize(_ context.Context, req AuthorizeRequest, now time.Time) (Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.holdByKey[req.IdempotencyKey]; ok {
		existing := s.holds[id]
		if existing.FromAccountID != req.FromAccountID ||
			existing.ToAccountID != req.ToAccountID ||
			existing.Amount != req.Amount {
			return Hold{}, ErrIdempotencyConflict
		}
		return existing, nil
	}

	from, ok := s.accounts[req.FromAccountID]
	if !ok {
		return Hold{}, ErrAccountNotFound
	}
	to, ok := s.accounts[req.ToAccountID]
	if !ok {
		return Hold{}, ErrAccountNotFound
	}
	if from.Currency != to.Currency {
		return Hold{}, ErrCurrencyMismatch
	}
	if from.Available() < req.Amount {
		return Hold{}, ErrInsufficientFunds
	}

	from.Held += req.Amount
	s.accounts[from.ID] = from

	id := newID()
	h := Hold{
		ID:             id,
		IdempotencyKey: req.IdempotencyKey,
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		Amount:         req.Amount,
		Status:         HoldActive,
		CreatedAt:      now,
	}
	if req.ExpiresIn > 0 {
		h.ExpiresAt = now.Add(req.ExpiresIn)
	}
	s.holds[id] = h
	s.holdByKey[req.IdempotencyKey] = id
	return h, nil
}

// Capture settles all or part of an active hold. It debits the source by the
// captured amount, credits the destination, releases the entire reservation
// (so any uncaptured remainder returns to Available), and records a transfer.
func (s *MemStore) Capture(_ context.Context, req CaptureRequest, now time.Time) (Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.captureByKey[req.IdempotencyKey]; ok {
		return s.transfers[id], nil
	}

	h, ok := s.holds[req.HoldID]
	if !ok {
		return Transfer{}, ErrHoldNotFound
	}
	if h.Status != HoldActive {
		return Transfer{}, ErrHoldNotActive
	}
	if !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt) {
		s.releaseHold(&h, HoldExpired) // an expired hold cannot be captured
		return Transfer{}, ErrHoldExpired
	}
	if req.Amount <= 0 {
		return Transfer{}, ErrInvalidAmount
	}
	if req.Amount > h.Amount {
		return Transfer{}, ErrCaptureExceedsHold
	}

	from := s.accounts[h.FromAccountID]
	to := s.accounts[h.ToAccountID]

	tid := newID()
	t := Transfer{
		ID:             tid,
		IdempotencyKey: req.IdempotencyKey,
		FromAccountID:  h.FromAccountID,
		ToAccountID:    h.ToAccountID,
		Amount:         req.Amount,
		Currency:       from.Currency,
		Status:         StatusPosted,
		CreatedAt:      now,
		Entries: []Entry{
			{ID: newID(), TransferID: tid, AccountID: h.FromAccountID, Amount: -req.Amount, CreatedAt: now},
			{ID: newID(), TransferID: tid, AccountID: h.ToAccountID, Amount: req.Amount, CreatedAt: now},
		},
	}

	// Same invariant as ApplyTransfer: a capture writes entries, so they must
	// balance before any state changes.
	if err := AssertBalanced(t.Entries); err != nil {
		return Transfer{}, err
	}

	from.Balance -= req.Amount
	from.Held -= h.Amount // release the whole reservation; remainder returns to Available
	to.Balance += req.Amount
	s.accounts[from.ID] = from
	s.accounts[to.ID] = to

	s.transfers[tid] = t
	for _, e := range t.Entries {
		s.entriesByAccount[e.AccountID] = append(s.entriesByAccount[e.AccountID], e)
	}
	s.captureByKey[req.IdempotencyKey] = tid

	h.Status = HoldCaptured
	h.Captured = req.Amount
	h.CaptureTransferID = tid
	s.holds[h.ID] = h
	return t, nil
}

// Void releases an active hold without moving money. Voiding an already-voided
// hold is a no-op (idempotent).
func (s *MemStore) Void(_ context.Context, holdID string) (Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.holds[holdID]
	if !ok {
		return Hold{}, ErrHoldNotFound
	}
	if h.Status == HoldVoided {
		return h, nil
	}
	if h.Status != HoldActive {
		return Hold{}, ErrHoldNotActive
	}
	s.releaseHold(&h, HoldVoided)
	return h, nil
}

func (s *MemStore) GetHold(_ context.Context, id string) (Hold, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.holds[id]
	return h, ok, nil
}

// ExpireHolds releases every active hold whose deadline has passed.
func (s *MemStore) ExpireHolds(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, h := range s.holds {
		if h.Status == HoldActive && !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt) {
			s.releaseHold(&h, HoldExpired)
			count++
		}
	}
	return count, nil
}

// releaseHold returns a hold's reserved funds to its source account's available
// balance and marks the hold with a terminal status. The caller holds the mutex.
func (s *MemStore) releaseHold(h *Hold, status HoldStatus) {
	from := s.accounts[h.FromAccountID]
	from.Held -= h.Amount
	s.accounts[from.ID] = from
	h.Status = status
	s.holds[h.ID] = *h
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
