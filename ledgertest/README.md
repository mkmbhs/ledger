# ledgertest — a conformance suite for `ledger.Store` implementations

`ledgertest` is the executable specification of this module's storage
contract. The in-memory reference (`ledger.MemStore`) and the PostgreSQL store
in `postgres/` are both required to pass it — and so can yours:

```go
package mystore_test

import (
	"testing"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/ledgertest"
)

func TestMyStore_Conformance(t *testing.T) {
	ledgertest.Run(t, func(t *testing.T) ledger.Store {
		return mystore.New(...) // a fresh, EMPTY store per call
	})
}
```

The package has no build tag and no dependencies beyond the ledger module and
`testing`, so it compiles everywhere `go test ./...` runs.

## The factory contract

`Run` calls your `Factory` once per scenario. Each call must return a store
that is empty and ready: a new in-memory instance, or a handle onto a
just-truncated schema. Register teardown with `t.Cleanup`; fail setup with
`t.Fatal`. Scenarios run sequentially, but several of them hammer the store
from many goroutines — run the suite with `-race`.

## Claiming conformance

An implementation may state that it **passes `mkmbhs/ledger` conformance v1**
(`ledgertest.Version`) when `ledgertest.Run` is green under
`go test -race`. The catalog is **add-only within a major version**: scenarios
are never changed or removed while `Version` stays `"v1"`, so the claim keeps
meaning the same thing. New scenarios may be added in the same major version —
they tighten future claims, not past ones — and anything breaking moves to
`"v2"`.

`Scenarios()` exposes the catalog as `{Name, Run}` pairs for à-la-carte runs,
and `AssertInvariants` is exported so your own additional tests can end with
the same safety check every scenario here ends with: no negative balances,
`held <= balance`, each balance reconciles to opening + its entries, and the
total across accounts is conserved.

## Scenario catalog (v1, 20 scenarios)

Transfers:

- `transfer/basic` — a transfer moves money once, posts two balanced entries, and round-trips through `GetTransfer`.
- `transfer/idempotent-replay` — a repeated idempotency key returns the original transfer; money moves exactly once.
- `transfer/idempotency-conflict` — a reused key with different parameters is rejected without moving money.
- `transfer/concurrent-same-key` — 64 concurrent retries of one key apply exactly once and all observe the same result.
- `transfer/concurrent-conservation` — 200 concurrent transfers around a ring of accounts conserve the total; opposite-direction pairs must not deadlock.
- `transfer/insufficient-funds` — an overdraft attempt is rejected and changes nothing.
- `transfer/unknown-account-and-currency-mismatch` — sentinel errors for a missing account and mismatched currencies.
- `transfer/cannot-spend-held-funds` — a transfer can spend only available funds; active holds fence off theirs.

Accounts:

- `account/create-idempotent` — identical re-create is a no-op; a mismatched one is `ErrAccountExists`; a live balance is never reset.
- `account/entries-reconciliation` — an account's entries always sum to its balance delta; unknown accounts are `ErrAccountNotFound`.

Holds:

- `hold/authorize-reserves-without-moving` — a hold raises `Held` (lowering `Available`) and moves nothing.
- `hold/authorize-idempotent-and-conflict` — replayed authorize returns the original hold; a mismatched key is rejected.
- `hold/authorize-insufficient-available` — a hold can only reserve available funds.
- `hold/capture-full` — full capture moves the money, releases the reservation, links hold to its settlement transfer.
- `hold/capture-partial-releases-remainder` — partial capture moves only the captured amount; the rest returns to available.
- `hold/capture-idempotent-concurrent` — 32 concurrent captures of one key settle exactly once.
- `hold/capture-errors` — over-capture, unknown hold, and capture-after-void each get their sentinel error.
- `hold/void-releases-and-double-void` — void releases without moving money; a second void never double-releases.
- `hold/expired-capture-fails` — an expired hold cannot be captured; the attempt releases the reservation and marks it expired.
- `hold/expire-sweep` — the sweep releases exactly the overdue holds, spares open-ended ones, and is idempotent.

Every scenario ends by checking the ledger invariants over the accounts it
touched, so a store cannot pass by getting the happy path right while quietly
corrupting a balance.
