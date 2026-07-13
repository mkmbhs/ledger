package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mkmbhs/ledger"
)

// newServer returns a live test server wrapping a fresh in-memory ledger.
func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewHandler(ledger.New(ledger.NewMemStore())))
	t.Cleanup(srv.Close)
	return srv
}

// do posts/gets JSON and returns the status code, decoding the body into out if
// non-nil.
func do(t *testing.T, method, url string, body any, out any) int {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return resp.StatusCode
}

// createAccount is a test helper that posts an account and asserts 201.
func createAccount(t *testing.T, base, id, currency string, opening int64) {
	t.Helper()
	code := do(t, http.MethodPost, base+"/v1/accounts", map[string]any{
		"id": id, "currency": currency, "opening": opening,
	}, nil)
	if code != http.StatusCreated {
		t.Fatalf("create account %s: status = %d, want 201", id, code)
	}
}

func TestHealthz(t *testing.T) {
	srv := newServer(t)
	if code := do(t, http.MethodGet, srv.URL+"/healthz", nil, nil); code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", code)
	}
}

func TestTransfer_HappyPath(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 1000)
	createAccount(t, srv.URL, "bob", "USD", 0)

	var tr struct {
		Transfer transferDTO `json:"transfer"`
	}
	code := do(t, http.MethodPost, srv.URL+"/v1/transfers", map[string]any{
		"idempotency_key": "t1", "from_account_id": "alice", "to_account_id": "bob", "amount": 300,
	}, &tr)
	if code != http.StatusCreated {
		t.Fatalf("transfer status = %d, want 201", code)
	}
	if tr.Transfer.Amount != 300 || tr.Transfer.Status != "posted" {
		t.Fatalf("transfer = %+v, want amount 300 posted", tr.Transfer)
	}
	if len(tr.Transfer.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(tr.Transfer.Entries))
	}

	// Balances reflect the move.
	var acct accountDTO
	do(t, http.MethodGet, srv.URL+"/v1/accounts/alice", nil, &acct)
	if acct.Balance != 700 || acct.Available != 700 {
		t.Fatalf("alice = %+v, want balance/available 700", acct)
	}

	// History exposes the entry.
	var hist struct {
		Entries []entryDTO `json:"entries"`
	}
	do(t, http.MethodGet, srv.URL+"/v1/accounts/bob/entries", nil, &hist)
	if len(hist.Entries) != 1 || hist.Entries[0].Amount != 300 {
		t.Fatalf("bob entries = %+v, want one +300 entry", hist.Entries)
	}
}

func TestHold_AuthorizeCaptureVoid(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 1000)
	createAccount(t, srv.URL, "bob", "USD", 0)

	var auth struct {
		Hold holdDTO `json:"hold"`
	}
	code := do(t, http.MethodPost, srv.URL+"/v1/holds", map[string]any{
		"idempotency_key": "h1", "from_account_id": "alice", "to_account_id": "bob", "amount": 400,
	}, &auth)
	if code != http.StatusCreated || auth.Hold.Status != "active" {
		t.Fatalf("authorize status = %d hold = %+v, want 201 active", code, auth.Hold)
	}

	// Funds reserved: available drops, balance unchanged.
	var acct accountDTO
	do(t, http.MethodGet, srv.URL+"/v1/accounts/alice", nil, &acct)
	if acct.Balance != 1000 || acct.Held != 400 || acct.Available != 600 {
		t.Fatalf("alice after hold = %+v, want balance 1000 held 400 available 600", acct)
	}

	// Capture part of it.
	var cap struct {
		Transfer transferDTO `json:"transfer"`
	}
	code = do(t, http.MethodPost, srv.URL+"/v1/holds/"+auth.Hold.ID+"/capture", map[string]any{
		"idempotency_key": "c1", "amount": 250,
	}, &cap)
	if code != http.StatusOK || cap.Transfer.Amount != 250 {
		t.Fatalf("capture status = %d transfer = %+v, want 200 amount 250", code, cap.Transfer)
	}

	// Reservation fully released; only the captured 250 moved.
	do(t, http.MethodGet, srv.URL+"/v1/accounts/alice", nil, &acct)
	if acct.Balance != 750 || acct.Held != 0 || acct.Available != 750 {
		t.Fatalf("alice after capture = %+v, want balance 750 held 0 available 750", acct)
	}

	// Void a second, fresh hold.
	do(t, http.MethodPost, srv.URL+"/v1/holds", map[string]any{
		"idempotency_key": "h2", "from_account_id": "alice", "to_account_id": "bob", "amount": 100,
	}, &auth)
	var voided struct {
		Hold holdDTO `json:"hold"`
	}
	code = do(t, http.MethodPost, srv.URL+"/v1/holds/"+auth.Hold.ID+"/void", nil, &voided)
	if code != http.StatusOK || voided.Hold.Status != "voided" {
		t.Fatalf("void status = %d hold = %+v, want 200 voided", code, voided.Hold)
	}
}

func TestErrorMapping_404AccountNotFound(t *testing.T) {
	srv := newServer(t)
	var body errorBody
	code := do(t, http.MethodGet, srv.URL+"/v1/accounts/ghost", nil, &body)
	if code != http.StatusNotFound || body.Code != "account_not_found" {
		t.Fatalf("missing account = %d %q, want 404 account_not_found", code, body.Code)
	}
}

func TestErrorMapping_404HoldNotFound(t *testing.T) {
	srv := newServer(t)
	var body errorBody
	code := do(t, http.MethodPost, srv.URL+"/v1/holds/ghost/void", nil, &body)
	if code != http.StatusNotFound || body.Code != "hold_not_found" {
		t.Fatalf("missing hold = %d %q, want 404 hold_not_found", code, body.Code)
	}
}

func TestErrorMapping_409IdempotencyConflict(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 1000)
	createAccount(t, srv.URL, "bob", "USD", 0)

	// First transfer succeeds.
	do(t, http.MethodPost, srv.URL+"/v1/transfers", map[string]any{
		"idempotency_key": "dup", "from_account_id": "alice", "to_account_id": "bob", "amount": 100,
	}, nil)
	// Same key, different amount -> conflict.
	var body errorBody
	code := do(t, http.MethodPost, srv.URL+"/v1/transfers", map[string]any{
		"idempotency_key": "dup", "from_account_id": "alice", "to_account_id": "bob", "amount": 200,
	}, &body)
	if code != http.StatusConflict || body.Code != "idempotency_conflict" {
		t.Fatalf("conflict = %d %q, want 409 idempotency_conflict", code, body.Code)
	}
}

func TestErrorMapping_400InvalidAmount(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 1000)
	createAccount(t, srv.URL, "bob", "USD", 0)

	var body errorBody
	code := do(t, http.MethodPost, srv.URL+"/v1/transfers", map[string]any{
		"idempotency_key": "t1", "from_account_id": "alice", "to_account_id": "bob", "amount": 0,
	}, &body)
	if code != http.StatusBadRequest || body.Code != "invalid_amount" {
		t.Fatalf("zero amount = %d %q, want 400 invalid_amount", code, body.Code)
	}
}

func TestErrorMapping_400SameAccount(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 1000)

	var body errorBody
	code := do(t, http.MethodPost, srv.URL+"/v1/transfers", map[string]any{
		"idempotency_key": "t1", "from_account_id": "alice", "to_account_id": "alice", "amount": 100,
	}, &body)
	if code != http.StatusBadRequest || body.Code != "same_account" {
		t.Fatalf("same account = %d %q, want 400 same_account", code, body.Code)
	}
}

func TestErrorMapping_400InsufficientFunds(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 100)
	createAccount(t, srv.URL, "bob", "USD", 0)

	var body errorBody
	code := do(t, http.MethodPost, srv.URL+"/v1/transfers", map[string]any{
		"idempotency_key": "t1", "from_account_id": "alice", "to_account_id": "bob", "amount": 500,
	}, &body)
	if code != http.StatusBadRequest || body.Code != "insufficient_funds" {
		t.Fatalf("overdraft = %d %q, want 400 insufficient_funds", code, body.Code)
	}
}

// TestCreateAccount_Conflict maps the idempotent-create semantics onto HTTP: an
// identical re-create is 201 again, a mismatched one is 409 account_exists.
func TestCreateAccount_Conflict(t *testing.T) {
	srv := newServer(t)
	createAccount(t, srv.URL, "alice", "USD", 1000)
	createAccount(t, srv.URL, "alice", "USD", 1000) // identical retry: still 201

	var errResp errorBody
	code := do(t, http.MethodPost, srv.URL+"/v1/accounts", map[string]any{
		"id": "alice", "currency": "USD", "opening": 5,
	}, &errResp)
	if code != http.StatusConflict {
		t.Fatalf("mismatched re-create: status = %d, want 409", code)
	}
	if errResp.Code != "account_exists" {
		t.Errorf("error code = %q, want account_exists", errResp.Code)
	}
}
