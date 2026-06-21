// Package rest exposes the ledger Service over a JSON/HTTP API built on the
// Go 1.22 net/http method+pattern router. It is a thin transport layer: it
// decodes a request, calls the Service, and maps ledger sentinel errors onto
// HTTP status codes. Every business rule lives in the Service, not here.
package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/mkmbhs/ledger/internal/ledger"
)

// NewHandler returns an http.Handler routing the ledger HTTP API to svc.
func NewHandler(svc *ledger.Service) http.Handler {
	h := &handler{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/transfers", h.createTransfer)
	mux.HandleFunc("POST /v1/holds", h.createHold)
	mux.HandleFunc("POST /v1/holds/{id}/capture", h.captureHold)
	mux.HandleFunc("POST /v1/holds/{id}/void", h.voidHold)
	mux.HandleFunc("GET /v1/accounts/{id}", h.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/entries", h.getEntries)
	mux.HandleFunc("POST /v1/accounts", h.createAccount)
	mux.HandleFunc("GET /healthz", h.healthz)
	return mux
}

// handler binds the Service to the HTTP routes. It holds no state of its own.
type handler struct{ svc *ledger.Service }

func (h *handler) createTransfer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		FromAccountID  string `json:"from_account_id"`
		ToAccountID    string `json:"to_account_id"`
		Amount         int64  `json:"amount"`
	}
	if !decode(w, r, &body) {
		return
	}
	t, err := h.svc.Transfer(r.Context(), ledger.TransferRequest{
		IdempotencyKey: body.IdempotencyKey,
		FromAccountID:  body.FromAccountID,
		ToAccountID:    body.ToAccountID,
		Amount:         ledger.Money(body.Amount),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"transfer": toTransferDTO(t)})
}

func (h *handler) createHold(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IdempotencyKey   string `json:"idempotency_key"`
		FromAccountID    string `json:"from_account_id"`
		ToAccountID      string `json:"to_account_id"`
		Amount           int64  `json:"amount"`
		ExpiresInSeconds int64  `json:"expires_in_seconds"`
	}
	if !decode(w, r, &body) {
		return
	}
	req := ledger.AuthorizeRequest{
		IdempotencyKey: body.IdempotencyKey,
		FromAccountID:  body.FromAccountID,
		ToAccountID:    body.ToAccountID,
		Amount:         ledger.Money(body.Amount),
	}
	if body.ExpiresInSeconds > 0 {
		req.ExpiresIn = time.Duration(body.ExpiresInSeconds) * time.Second
	}
	hold, err := h.svc.Authorize(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"hold": toHoldDTO(hold)})
}

func (h *handler) captureHold(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		Amount         int64  `json:"amount"`
	}
	if !decode(w, r, &body) {
		return
	}
	t, err := h.svc.Capture(r.Context(), ledger.CaptureRequest{
		IdempotencyKey: body.IdempotencyKey,
		HoldID:         r.PathValue("id"),
		Amount:         ledger.Money(body.Amount),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"transfer": toTransferDTO(t)})
}

func (h *handler) voidHold(w http.ResponseWriter, r *http.Request) {
	hold, err := h.svc.Void(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hold": toHoldDTO(hold)})
}

func (h *handler) getAccount(w http.ResponseWriter, r *http.Request) {
	a, err := h.svc.Account(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toAccountDTO(a))
}

func (h *handler) getEntries(w http.ResponseWriter, r *http.Request) {
	entries, err := h.svc.AccountHistory(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]entryDTO, len(entries))
	for i, e := range entries {
		out[i] = toEntryDTO(e)
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": out})
}

func (h *handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID       string `json:"id"`
		Currency string `json:"currency"`
		Opening  int64  `json:"opening"`
	}
	if !decode(w, r, &body) {
		return
	}
	if err := h.svc.CreateAccount(r.Context(), body.ID, body.Currency, ledger.Money(body.Opening)); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, accountDTO{
		ID:        body.ID,
		Currency:  body.Currency,
		Balance:   body.Opening,
		Held:      0,
		Available: body.Opening,
	})
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// decode reads a JSON request body into v, writing a 400 and returning false on
// malformed input so the caller can simply return.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error(), Code: "invalid_request"})
		return false
	}
	return true
}

// writeJSON encodes v as the response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// errorBody is the JSON shape returned for every error: a human-readable message
// and a stable machine code clients can switch on.
type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// writeError maps a ledger error to its HTTP status and code and writes it.
func writeError(w http.ResponseWriter, err error) {
	status, code := statusForError(err)
	writeJSON(w, status, errorBody{Error: err.Error(), Code: code})
}

// statusForError translates ledger sentinel errors into an HTTP status and a
// stable error code. Unknown errors are treated as a 500 so a bug never leaks
// as a client error.
func statusForError(err error) (int, string) {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		return http.StatusNotFound, "account_not_found"
	case errors.Is(err, ledger.ErrHoldNotFound):
		return http.StatusNotFound, "hold_not_found"
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		return http.StatusConflict, "idempotency_conflict"
	case errors.Is(err, ledger.ErrInvalidAmount):
		return http.StatusBadRequest, "invalid_amount"
	case errors.Is(err, ledger.ErrSameAccount):
		return http.StatusBadRequest, "same_account"
	case errors.Is(err, ledger.ErrMissingIdempotencyKey):
		return http.StatusBadRequest, "missing_idempotency_key"
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		return http.StatusBadRequest, "currency_mismatch"
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return http.StatusBadRequest, "insufficient_funds"
	case errors.Is(err, ledger.ErrCaptureExceedsHold):
		return http.StatusBadRequest, "capture_exceeds_hold"
	case errors.Is(err, ledger.ErrHoldNotActive):
		return http.StatusBadRequest, "hold_not_active"
	case errors.Is(err, ledger.ErrHoldExpired):
		return http.StatusBadRequest, "hold_expired"
	default:
		return http.StatusInternalServerError, "internal"
	}
}
