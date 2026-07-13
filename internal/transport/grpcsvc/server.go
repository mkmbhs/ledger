// Package grpcsvc adapts the ledger domain service to gRPC. It is a thin
// transport layer: it translates protobuf requests into domain calls, maps
// domain sentinel errors onto gRPC status codes, and translates domain results
// back into protobuf. All business rules live in the root ledger package.
package grpcsvc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/internal/transport/grpcsvc/ledgerv1"
)

// Server implements ledgerv1.LedgerServiceServer over a *ledger.Service.
type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc *ledger.Service
}

// NewServer wraps a ledger.Service in a gRPC server adapter.
func NewServer(svc *ledger.Service) *Server { return &Server{svc: svc} }

// Register binds a Server backed by svc onto the given gRPC server.
func Register(gs *grpc.Server, svc *ledger.Service) {
	ledgerv1.RegisterLedgerServiceServer(gs, NewServer(svc))
}

func (s *Server) Transfer(ctx context.Context, req *ledgerv1.TransferRequest) (*ledgerv1.TransferResponse, error) {
	t, err := s.svc.Transfer(ctx, ledger.TransferRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		FromAccountID:  req.GetFromAccountId(),
		ToAccountID:    req.GetToAccountId(),
		Amount:         ledger.Money(req.GetAmount()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &ledgerv1.TransferResponse{Transfer: transferToProto(t)}, nil
}

func (s *Server) Authorize(ctx context.Context, req *ledgerv1.AuthorizeRequest) (*ledgerv1.AuthorizeResponse, error) {
	h, err := s.svc.Authorize(ctx, ledger.AuthorizeRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		FromAccountID:  req.GetFromAccountId(),
		ToAccountID:    req.GetToAccountId(),
		Amount:         ledger.Money(req.GetAmount()),
		ExpiresIn:      time.Duration(req.GetExpiresInSeconds()) * time.Second,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &ledgerv1.AuthorizeResponse{Hold: holdToProto(h)}, nil
}

func (s *Server) Capture(ctx context.Context, req *ledgerv1.CaptureRequest) (*ledgerv1.CaptureResponse, error) {
	t, err := s.svc.Capture(ctx, ledger.CaptureRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		HoldID:         req.GetHoldId(),
		Amount:         ledger.Money(req.GetAmount()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &ledgerv1.CaptureResponse{Transfer: transferToProto(t)}, nil
}

func (s *Server) Void(ctx context.Context, req *ledgerv1.VoidRequest) (*ledgerv1.VoidResponse, error) {
	h, err := s.svc.Void(ctx, req.GetHoldId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &ledgerv1.VoidResponse{Hold: holdToProto(h)}, nil
}

func (s *Server) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.GetAccountResponse, error) {
	a, err := s.svc.Account(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &ledgerv1.GetAccountResponse{Account: accountToProto(a)}, nil
}

func (s *Server) GetAccountEntries(ctx context.Context, req *ledgerv1.GetAccountEntriesRequest) (*ledgerv1.GetAccountEntriesResponse, error) {
	entries, err := s.svc.AccountHistory(ctx, req.GetAccountId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*ledgerv1.Entry, len(entries))
	for i, e := range entries {
		out[i] = entryToProto(e)
	}
	return &ledgerv1.GetAccountEntriesResponse{Entries: out}, nil
}

// toStatus maps a domain sentinel error onto a gRPC status. Validation failures
// become InvalidArgument, lookups become NotFound, idempotency reuse becomes
// AlreadyExists, and broken business preconditions become FailedPrecondition.
// Anything unrecognized is an Internal error.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ledger.ErrAccountNotFound),
		errors.Is(err, ledger.ErrHoldNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ledger.ErrIdempotencyConflict),
		errors.Is(err, ledger.ErrAccountExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrSameAccount),
		errors.Is(err, ledger.ErrMissingIdempotencyKey),
		errors.Is(err, ledger.ErrCurrencyMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds),
		errors.Is(err, ledger.ErrCaptureExceedsHold),
		errors.Is(err, ledger.ErrHoldNotActive),
		errors.Is(err, ledger.ErrHoldExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func accountToProto(a ledger.Account) *ledgerv1.Account {
	return &ledgerv1.Account{
		Id:        a.ID,
		Currency:  a.Currency,
		Balance:   int64(a.Balance),
		Held:      int64(a.Held),
		Available: int64(a.Available()),
	}
}

func entryToProto(e ledger.Entry) *ledgerv1.Entry {
	return &ledgerv1.Entry{
		Id:         e.ID,
		TransferId: e.TransferID,
		AccountId:  e.AccountID,
		Amount:     int64(e.Amount),
		CreatedAt:  ts(e.CreatedAt),
	}
}

func transferToProto(t ledger.Transfer) *ledgerv1.Transfer {
	entries := make([]*ledgerv1.Entry, len(t.Entries))
	for i, e := range t.Entries {
		entries[i] = entryToProto(e)
	}
	return &ledgerv1.Transfer{
		Id:             t.ID,
		IdempotencyKey: t.IdempotencyKey,
		FromAccountId:  t.FromAccountID,
		ToAccountId:    t.ToAccountID,
		Amount:         int64(t.Amount),
		Currency:       t.Currency,
		Status:         transferStatusToProto(t.Status),
		CreatedAt:      ts(t.CreatedAt),
		Entries:        entries,
	}
}

func holdToProto(h ledger.Hold) *ledgerv1.Hold {
	return &ledgerv1.Hold{
		Id:                h.ID,
		IdempotencyKey:    h.IdempotencyKey,
		FromAccountId:     h.FromAccountID,
		ToAccountId:       h.ToAccountID,
		Amount:            int64(h.Amount),
		Captured:          int64(h.Captured),
		Status:            holdStatusToProto(h.Status),
		CreatedAt:         ts(h.CreatedAt),
		ExpiresAt:         ts(h.ExpiresAt),
		CaptureTransferId: h.CaptureTransferID,
	}
}

func transferStatusToProto(s ledger.TransferStatus) ledgerv1.TransferStatus {
	switch s {
	case ledger.StatusPosted:
		return ledgerv1.TransferStatus_TRANSFER_STATUS_POSTED
	default:
		return ledgerv1.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
	}
}

func holdStatusToProto(s ledger.HoldStatus) ledgerv1.HoldStatus {
	switch s {
	case ledger.HoldActive:
		return ledgerv1.HoldStatus_HOLD_STATUS_ACTIVE
	case ledger.HoldCaptured:
		return ledgerv1.HoldStatus_HOLD_STATUS_CAPTURED
	case ledger.HoldVoided:
		return ledgerv1.HoldStatus_HOLD_STATUS_VOIDED
	case ledger.HoldExpired:
		return ledgerv1.HoldStatus_HOLD_STATUS_EXPIRED
	default:
		return ledgerv1.HoldStatus_HOLD_STATUS_UNSPECIFIED
	}
}

// ts converts a time to a protobuf timestamp, mapping the zero time (an unset
// optional time such as a hold with no expiry) to a nil timestamp.
func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
