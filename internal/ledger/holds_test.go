package ledger

import (
	"context"
	"errors"
	"testing"
	"time"
)

func getAccount(t *testing.T, s *Service, id string) Account {
	t.Helper()
	a, err := s.Account(context.Background(), id)
	if err != nil {
		t.Fatalf("Account(%s): %v", id, err)
	}
	return a
}

func authorize(t *testing.T, s *Service, key, from, to string, amt Money) Hold {
	t.Helper()
	h, err := s.Authorize(context.Background(), AuthorizeRequest{
		IdempotencyKey: key, FromAccountID: from, ToAccountID: to, Amount: amt,
	})
	if err != nil {
		t.Fatalf("Authorize(%s): %v", key, err)
	}
	return h
}

func TestAuthorize_ReservesWithoutMoving(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	h := authorize(t, s, "a1", "alice", "bob", 300)
	if h.Status != HoldActive {
		t.Errorf("status = %q, want active", h.Status)
	}
	alice := getAccount(t, s, "alice")
	if alice.Balance != 1000 {
		t.Errorf("balance moved before capture: %d", alice.Balance)
	}
	if alice.Held != 300 || alice.Available() != 700 {
		t.Errorf("held=%d available=%d, want 300/700", alice.Held, alice.Available())
	}
	if got := balance(t, s, "bob"); got != 0 {
		t.Errorf("bob received money before capture: %d", got)
	}
}

func TestCapture_Full(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300)
	tr, err := s.Capture(ctx, CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if tr.Amount != 300 {
		t.Errorf("transfer amount = %d, want 300", tr.Amount)
	}
	alice := getAccount(t, s, "alice")
	if alice.Balance != 700 || alice.Held != 0 {
		t.Errorf("alice balance=%d held=%d, want 700/0", alice.Balance, alice.Held)
	}
	if got := balance(t, s, "bob"); got != 300 {
		t.Errorf("bob = %d, want 300", got)
	}
	if hold, _, _ := s.GetHold(ctx, h.ID); hold.Status != HoldCaptured || hold.Captured != 300 {
		t.Errorf("hold = %q/%d, want captured/300", hold.Status, hold.Captured)
	}
}

func TestCapture_Partial_ReleasesRemainder(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	h := authorize(t, s, "a", "alice", "bob", 300)
	if _, err := s.Capture(context.Background(), CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 100}); err != nil {
		t.Fatal(err)
	}
	alice := getAccount(t, s, "alice")
	// Only 100 moved; the 200 remainder is released back to available.
	if alice.Balance != 900 || alice.Held != 0 || alice.Available() != 900 {
		t.Errorf("alice balance=%d held=%d available=%d, want 900/0/900", alice.Balance, alice.Held, alice.Available())
	}
	if got := balance(t, s, "bob"); got != 100 {
		t.Errorf("bob = %d, want 100", got)
	}
}

func TestVoid_ReleasesFunds(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300)
	vh, err := s.Void(ctx, h.ID)
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if vh.Status != HoldVoided {
		t.Errorf("status = %q, want voided", vh.Status)
	}
	alice := getAccount(t, s, "alice")
	if alice.Balance != 1000 || alice.Held != 0 || alice.Available() != 1000 {
		t.Errorf("alice not fully released: balance=%d held=%d", alice.Balance, alice.Held)
	}

	// Voiding again is a no-op and must not double-release.
	if _, err := s.Void(ctx, h.ID); err != nil {
		t.Fatalf("second Void: %v", err)
	}
	if a := getAccount(t, s, "alice"); a.Held != 0 {
		t.Errorf("double Void corrupted held: %d", a.Held)
	}
}

func TestAuthorize_InsufficientAvailable(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	authorize(t, s, "a1", "alice", "bob", 700) // available now 300
	_, err := s.Authorize(context.Background(), AuthorizeRequest{
		IdempotencyKey: "a2", FromAccountID: "alice", ToAccountID: "bob", Amount: 400,
	})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("err = %v, want ErrInsufficientFunds", err)
	}
}

func TestAuthorize_Idempotent(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)

	h1 := authorize(t, s, "same", "alice", "bob", 300)
	h2 := authorize(t, s, "same", "alice", "bob", 300)
	if h1.ID != h2.ID {
		t.Errorf("idempotent authorize made two holds: %s vs %s", h1.ID, h2.ID)
	}
	if a := getAccount(t, s, "alice"); a.Held != 300 {
		t.Errorf("held = %d, want 300 (reserved once)", a.Held)
	}
}

func TestCapture_Idempotent(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300)
	req := CaptureRequest{IdempotencyKey: "cap", HoldID: h.ID, Amount: 300}
	t1, err := s.Capture(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := s.Capture(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if t1.ID != t2.ID {
		t.Errorf("idempotent capture made two transfers")
	}
	if got := balance(t, s, "bob"); got != 300 {
		t.Errorf("bob = %d, want 300 (captured once)", got)
	}
}

func TestCapture_Errors(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	h := authorize(t, s, "a", "alice", "bob", 300)

	if _, err := s.Capture(ctx, CaptureRequest{IdempotencyKey: "x", HoldID: h.ID, Amount: 400}); !errors.Is(err, ErrCaptureExceedsHold) {
		t.Errorf("over-capture err = %v, want ErrCaptureExceedsHold", err)
	}
	if _, err := s.Capture(ctx, CaptureRequest{IdempotencyKey: "y", HoldID: "ghost", Amount: 10}); !errors.Is(err, ErrHoldNotFound) {
		t.Errorf("unknown hold err = %v, want ErrHoldNotFound", err)
	}

	// Void, then capture -> not active.
	if _, err := s.Void(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Capture(ctx, CaptureRequest{IdempotencyKey: "z", HoldID: h.ID, Amount: 100}); !errors.Is(err, ErrHoldNotActive) {
		t.Errorf("capture-after-void err = %v, want ErrHoldNotActive", err)
	}
}

func TestHoldExpiry(t *testing.T) {
	s := newService(t)
	base := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return base }
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	h, err := s.Authorize(ctx, AuthorizeRequest{
		IdempotencyKey: "a", FromAccountID: "alice", ToAccountID: "bob", Amount: 300, ExpiresIn: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Move the clock past the deadline; capture must now fail and release funds.
	s.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, err := s.Capture(ctx, CaptureRequest{IdempotencyKey: "c", HoldID: h.ID, Amount: 300}); !errors.Is(err, ErrHoldExpired) {
		t.Errorf("capture err = %v, want ErrHoldExpired", err)
	}
	if a := getAccount(t, s, "alice"); a.Held != 0 || a.Available() != 1000 {
		t.Errorf("expired hold not released: held=%d", a.Held)
	}
	if hold, _, _ := s.GetHold(ctx, h.ID); hold.Status != HoldExpired {
		t.Errorf("hold status = %q, want expired", hold.Status)
	}
}

func TestExpireHolds_Sweep(t *testing.T) {
	s := newService(t)
	base := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return base }
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	for _, k := range []string{"h1", "h2"} {
		if _, err := s.Authorize(ctx, AuthorizeRequest{
			IdempotencyKey: k, FromAccountID: "alice", ToAccountID: "bob", Amount: 100, ExpiresIn: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if a := getAccount(t, s, "alice"); a.Held != 200 {
		t.Fatalf("held = %d, want 200", a.Held)
	}

	s.now = func() time.Time { return base.Add(2 * time.Minute) }
	n, err := s.ExpireHolds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expired %d, want 2", n)
	}
	if a := getAccount(t, s, "alice"); a.Held != 0 {
		t.Errorf("held after sweep = %d, want 0", a.Held)
	}
}

// TestTransfer_RespectsHolds proves a direct transfer cannot spend funds that an
// active hold has reserved.
func TestTransfer_RespectsHolds(t *testing.T) {
	s := newService(t)
	mustAccount(t, s, "alice", "USD", 1000)
	mustAccount(t, s, "bob", "USD", 0)
	ctx := context.Background()

	authorize(t, s, "hold", "alice", "bob", 800) // available now 200

	if _, err := s.Transfer(ctx, TransferRequest{IdempotencyKey: "t1", FromAccountID: "alice", ToAccountID: "bob", Amount: 500}); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("transfer of held funds err = %v, want ErrInsufficientFunds", err)
	}
	// A transfer within the available balance still works.
	if _, err := s.Transfer(ctx, TransferRequest{IdempotencyKey: "t2", FromAccountID: "alice", ToAccountID: "bob", Amount: 200}); err != nil {
		t.Errorf("transfer within available failed: %v", err)
	}
}
