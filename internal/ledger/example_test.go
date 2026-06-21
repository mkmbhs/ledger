package ledger_test

import (
	"context"
	"fmt"

	"github.com/mkmbhs/ledger/internal/ledger"
)

// ExampleService_Transfer shows the core guarantee: a transfer applied twice with
// the same idempotency key takes effect only once. (This runs as a test and
// appears in godoc.)
func ExampleService_Transfer() {
	ctx := context.Background()
	svc := ledger.New(ledger.NewMemStore())
	_ = svc.CreateAccount(ctx, "alice", "USD", 1000)
	_ = svc.CreateAccount(ctx, "bob", "USD", 0)

	req := ledger.TransferRequest{
		IdempotencyKey: "checkout-42",
		FromAccountID:  "alice",
		ToAccountID:    "bob",
		Amount:         250,
	}
	_, _ = svc.Transfer(ctx, req) // first call applies
	_, _ = svc.Transfer(ctx, req) // retry is a no-op

	alice, _ := svc.Balance(ctx, "alice")
	bob, _ := svc.Balance(ctx, "bob")
	fmt.Printf("alice=%d bob=%d\n", alice, bob)
	// Output: alice=750 bob=250
}
