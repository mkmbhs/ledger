package ledger

import (
	"context"
	"fmt"
	"testing"
)

// FuzzTransfers throws randomized sequences of transfers at the ledger and
// asserts the three invariants that must ALWAYS hold, no matter the order,
// concurrency, or validity of the operations:
//
//  1. Conservation   — the sum of all balances never changes.
//  2. Reconciliation — every account's balance equals its opening balance plus
//     the sum of its entries (the books reconcile).
//  3. Zero-sum books — the entries across all accounts net to exactly zero
//     (double-entry: money is moved, never created or destroyed).
//
// Invariant testing like this finds the bugs example-based tests miss: it does
// not care what the "right" answer is for a given input, only that these
// properties are preserved for every input.
//
// Run: go test -run x -fuzz FuzzTransfers .
func FuzzTransfers(f *testing.F) {
	f.Add([]byte{0, 1, 50, 1, 2, 30, 2, 0, 200})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0}) // self-transfer + zero amount: both rejected, must not corrupt state

	f.Fuzz(func(t *testing.T, data []byte) {
		ctx := context.Background()
		s := New(NewMemStore())

		const n = 5
		const opening Money = 1000
		for i := range n {
			if err := s.CreateAccount(ctx, acctID(i), "USD", opening); err != nil {
				t.Fatal(err)
			}
		}

		// Interpret the fuzz input as a sequence of (from, to, amount) triples.
		// Invalid operations (self-transfer, zero amount, insufficient funds) are
		// expected and simply rejected — they must never change the books.
		for i := 0; i+2 < len(data); i += 3 {
			_, _ = s.Transfer(ctx, TransferRequest{
				IdempotencyKey: fmt.Sprintf("op-%d", i),
				FromAccountID:  acctID(int(data[i]) % n),
				ToAccountID:    acctID(int(data[i+1]) % n),
				Amount:         Money(data[i+2]),
			})
		}

		var totalBalance, totalEntries Money
		for i := range n {
			id := acctID(i)
			bal, err := s.Balance(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if bal < 0 {
				t.Fatalf("invariant violated: negative balance for %s: %d", id, bal)
			}
			entries, err := s.AccountHistory(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			var sum Money
			for _, e := range entries {
				sum += e.Amount
			}
			if opening+sum != bal { // invariant 2
				t.Fatalf("invariant violated: %s opening(%d)+entries(%d) != balance(%d)", id, opening, sum, bal)
			}
			totalBalance += bal
			totalEntries += sum
		}
		if totalBalance != n*opening { // invariant 1
			t.Fatalf("invariant violated: money not conserved: total=%d want=%d", totalBalance, n*opening)
		}
		if totalEntries != 0 { // invariant 3
			t.Fatalf("invariant violated: entries do not net to zero across the ledger: %d", totalEntries)
		}
	})
}

func acctID(i int) string { return fmt.Sprintf("a%d", i) }
