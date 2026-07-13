// Command ledger is a tiny demo of the ledger package: it shows idempotent
// transfers and the authorization-hold lifecycle. Run it with: go run ./cmd/ledger
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/mkmbhs/ledger"
)

func main() {
	ctx := context.Background()
	svc := ledger.New(ledger.NewMemStore())
	must(svc.CreateAccount(ctx, "alice", "USD", 1000))
	must(svc.CreateAccount(ctx, "bob", "USD", 0))

	// 1) Idempotency: the same request applied twice takes effect once.
	fmt.Println("== idempotent transfer ==")
	req := ledger.TransferRequest{IdempotencyKey: "demo-1", FromAccountID: "alice", ToAccountID: "bob", Amount: 250}
	for i := 1; i <= 2; i++ {
		tr, err := svc.Transfer(ctx, req)
		must(err)
		fmt.Printf("attempt %d -> transfer %s\n", i, tr.ID[:8])
	}
	show(ctx, svc, "alice", "bob")
	fmt.Println("alice debited exactly once despite two identical requests.")

	// 2) Hold lifecycle: reserve 200, then capture only 120; the rest is released.
	fmt.Println("\n== authorization hold ==")
	h, err := svc.Authorize(ctx, ledger.AuthorizeRequest{
		IdempotencyKey: "auth-1", FromAccountID: "alice", ToAccountID: "bob", Amount: 200,
	})
	must(err)
	fmt.Printf("authorized hold %s for 200\n", h.ID[:8])
	show(ctx, svc, "alice", "bob") // balance unchanged, available drops by 200

	_, err = svc.Capture(ctx, ledger.CaptureRequest{IdempotencyKey: "cap-1", HoldID: h.ID, Amount: 120})
	must(err)
	fmt.Println("captured 120 (the remaining 80 is released)")
	show(ctx, svc, "alice", "bob")
}

func show(ctx context.Context, svc *ledger.Service, ids ...string) {
	for _, id := range ids {
		a, err := svc.Account(ctx, id)
		must(err)
		fmt.Printf("  %-6s balance=%-4d held=%-3d available=%d\n", id, a.Balance, a.Held, a.Available())
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
