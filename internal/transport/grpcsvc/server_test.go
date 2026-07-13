package grpcsvc_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/internal/transport/grpcsvc"
	"github.com/mkmbhs/ledger/internal/transport/grpcsvc/ledgerv1"
)

// newClient stands up an in-memory gRPC server (bufconn, no network or DB) over
// a memstore-backed ledger.Service and returns a connected client. The svc is
// returned too so a test can seed accounts directly.
func newClient(t *testing.T) (ledgerv1.LedgerServiceClient, *ledger.Service) {
	t.Helper()

	svc := ledger.New(ledger.NewMemStore())
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	grpcsvc.Register(gs, svc)

	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return ledgerv1.NewLedgerServiceClient(conn), svc
}

func TestServer_TransferAndAuthorizeCapture(t *testing.T) {
	ctx := context.Background()
	client, svc := newClient(t)

	if err := svc.CreateAccount(ctx, "alice", "USD", 1000); err != nil {
		t.Fatalf("create alice: %v", err)
	}
	if err := svc.CreateAccount(ctx, "bob", "USD", 0); err != nil {
		t.Fatalf("create bob: %v", err)
	}

	// Transfer 300 alice -> bob.
	tr, err := client.Transfer(ctx, &ledgerv1.TransferRequest{
		IdempotencyKey: "t1",
		FromAccountId:  "alice",
		ToAccountId:    "bob",
		Amount:         300,
	})
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	if got := tr.GetTransfer().GetAmount(); got != 300 {
		t.Fatalf("transfer amount = %d, want 300", got)
	}
	if n := len(tr.GetTransfer().GetEntries()); n != 2 {
		t.Fatalf("transfer entries = %d, want 2", n)
	}

	// Authorize a 200 hold alice -> bob, then capture 150 of it.
	auth, err := client.Authorize(ctx, &ledgerv1.AuthorizeRequest{
		IdempotencyKey: "a1",
		FromAccountId:  "alice",
		ToAccountId:    "bob",
		Amount:         200,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	holdID := auth.GetHold().GetId()
	if auth.GetHold().GetStatus() != ledgerv1.HoldStatus_HOLD_STATUS_ACTIVE {
		t.Fatalf("hold status = %v, want ACTIVE", auth.GetHold().GetStatus())
	}

	// While the hold is active alice's available = 1000 - 300 - 200 = 500.
	acc, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: "alice"})
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if got := acc.GetAccount().GetAvailable(); got != 500 {
		t.Fatalf("alice available = %d, want 500", got)
	}
	if got := acc.GetAccount().GetHeld(); got != 200 {
		t.Fatalf("alice held = %d, want 200", got)
	}

	cap, err := client.Capture(ctx, &ledgerv1.CaptureRequest{
		IdempotencyKey: "c1",
		HoldId:         holdID,
		Amount:         150,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if got := cap.GetTransfer().GetAmount(); got != 150 {
		t.Fatalf("capture amount = %d, want 150", got)
	}

	// Final: alice balance = 1000 - 300 - 150 = 550, hold released, bob = 450.
	acc, _ = client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: "alice"})
	if got := acc.GetAccount().GetBalance(); got != 550 {
		t.Fatalf("alice balance = %d, want 550", got)
	}
	if got := acc.GetAccount().GetHeld(); got != 0 {
		t.Fatalf("alice held = %d, want 0", got)
	}
	bob, _ := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: "bob"})
	if got := bob.GetAccount().GetBalance(); got != 450 {
		t.Fatalf("bob balance = %d, want 450", got)
	}
}

func TestServer_ErrorMapping(t *testing.T) {
	ctx := context.Background()
	client, svc := newClient(t)

	if err := svc.CreateAccount(ctx, "alice", "USD", 100); err != nil {
		t.Fatalf("create alice: %v", err)
	}
	if err := svc.CreateAccount(ctx, "bob", "USD", 0); err != nil {
		t.Fatalf("create bob: %v", err)
	}

	// Transfer to a missing account -> ErrAccountNotFound -> NotFound.
	_, err := client.Transfer(ctx, &ledgerv1.TransferRequest{
		IdempotencyKey: "t-missing",
		FromAccountId:  "alice",
		ToAccountId:    "ghost",
		Amount:         10,
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("missing account: code = %v, want NotFound (err=%v)", got, err)
	}

	// Overdraw -> ErrInsufficientFunds -> FailedPrecondition.
	_, err = client.Transfer(ctx, &ledgerv1.TransferRequest{
		IdempotencyKey: "t-overdraw",
		FromAccountId:  "alice",
		ToAccountId:    "bob",
		Amount:         1_000_000,
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("overdraw: code = %v, want FailedPrecondition (err=%v)", got, err)
	}

	// Missing idempotency key -> ErrMissingIdempotencyKey -> InvalidArgument.
	_, err = client.Transfer(ctx, &ledgerv1.TransferRequest{
		FromAccountId: "alice",
		ToAccountId:   "bob",
		Amount:        10,
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("missing key: code = %v, want InvalidArgument (err=%v)", got, err)
	}
}

// TestCreatePosting exercises the multi-leg gRPC surface end to end: a fee
// split applies atomically and an unbalanced set is InvalidArgument.
func TestCreatePosting(t *testing.T) {
	client, svc := newClient(t)
	ctx := context.Background()
	if err := svc.CreateAccount(ctx, "alice", "USD", 1000); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAccount(ctx, "merchant", "USD", 0); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAccount(ctx, "fees", "USD", 0); err != nil {
		t.Fatal(err)
	}

	resp, err := client.CreatePosting(ctx, &ledgerv1.CreatePostingRequest{
		IdempotencyKey: "split",
		Postings: []*ledgerv1.Posting{
			{AccountId: "alice", Amount: -100},
			{AccountId: "merchant", Amount: 97},
			{AccountId: "fees", Amount: 3},
		},
	})
	if err != nil {
		t.Fatalf("CreatePosting: %v", err)
	}
	if got := resp.GetTransfer(); len(got.GetEntries()) != 3 || got.GetFromAccountId() != "" {
		t.Errorf("transfer = %+v, want 3 entries and empty two-leg summary", got)
	}

	acct, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: "merchant"})
	if err != nil {
		t.Fatal(err)
	}
	if acct.GetAccount().GetBalance() != 97 {
		t.Errorf("merchant = %d, want 97", acct.GetAccount().GetBalance())
	}

	_, err = client.CreatePosting(ctx, &ledgerv1.CreatePostingRequest{
		IdempotencyKey: "bad",
		Postings: []*ledgerv1.Posting{
			{AccountId: "alice", Amount: -100},
			{AccountId: "merchant", Amount: 90},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("unbalanced err = %v, want InvalidArgument", err)
	}
}
