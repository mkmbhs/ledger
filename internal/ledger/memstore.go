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
	byIdemKey        map[string]string  // idempotency key -> transfer ID
	entriesByAccount map[string][]Entry // account ID -> its entries, oldest first
	now              func() time.Time
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		accounts:         make(map[string]Account),
		transfers:        make(map[string]Transfer),
		byIdemKey:        make(map[string]string),
		entriesByAccount: make(map[string][]Entry),
		now:              time.Now,
	}
}

func (s *MemStore) CreateAccount(_ context.Context, a Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if from.Balance < req.Amount {
		return Transfer{}, ErrInsufficientFunds
	}

	// Apply the balance changes.
	from.Balance -= req.Amount
	to.Balance += req.Amount
	s.accounts[from.ID] = from
	s.accounts[to.ID] = to

	// Record the transfer with its two balanced entries.
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
	// created nor destroyed). This is the heart of double-entry accounting.
	if err := assertBalanced(t.Entries); err != nil {
		return Transfer{}, err
	}

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

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
