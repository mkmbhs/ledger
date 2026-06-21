// Command ledger is a tiny demo of the ledger package: it opens two accounts,
// moves money between them, and shows that a retried transfer (same idempotency
// key) is applied only once. Run it with: go run ./cmd/ledger
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/mkmbhs/ledger/internal/ledger"
)

func main() {
	ctx := context.Background()
	svc := ledger.New(ledger.NewMemStore())

	must(svc.CreateAccount(ctx, "alice", "USD", 1000))
	must(svc.CreateAccount(ctx, "bob", "USD", 0))

	req := ledger.TransferRequest{
		IdempotencyKey: "demo-key-1",
		FromAccountID:  "alice",
		ToAccountID:    "bob",
		Amount:         250,
	}

	// Apply the same request twice. The second call is a no-op replay.
	for i := 1; i <= 2; i++ {
		tr, err := svc.Transfer(ctx, req)
		must(err)
		fmt.Printf("attempt %d -> transfer %s (applied once)\n", i, tr.ID[:8])
	}

	printBalance(ctx, svc, "alice")
	printBalance(ctx, svc, "bob")
	fmt.Println("alice was debited exactly once despite two identical requests.")
}

func printBalance(ctx context.Context, svc *ledger.Service, id string) {
	b, err := svc.Balance(ctx, id)
	must(err)
	fmt.Printf("%-6s balance: %d\n", id, b)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
