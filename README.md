# ledger

A small, correct **wallet / payments ledger** in Go. It does the two things that
make moving money hard — **transactional correctness** and **safe retries** —
and it models the real payment lifecycle with **authorization holds**
(`authorize → capture / void / expire`), not just instant transfers.

It is intentionally not a framework. It is a clear, tested core that shows how to
move money without ever creating it, destroying it, double-applying a request, or
spending funds that are already reserved.

```
make race     # go test -race ./...   the idempotency, concurrency + hold proofs
make run      # go run ./cmd/ledger   a tiny demo
```

## The problem

Every wallet, payments, or settlement system has to answer the same questions:

- If a client retries after a timeout, does the money move twice?
- If two operations touch one account at once, can a balance be lost?
- When you *authorize* a payment (or place a bet, or pre-auth a card), how do you
  fence off the funds so they can't be spent twice before the capture lands?
- Does every movement balance, so the books always reconcile?

This repo answers those with the smallest amount of code that makes the answers
obvious and testable.

## Core concepts

**Double-entry.** Every transfer posts two `entries` — a debit and a credit that
sum to zero. Money is never created or destroyed; the books always balance.

**Integer money.** Amounts are `int64` minor units (e.g. cents). Never a float.

**Available vs. settled balance.** An account has a settled `Balance` and a `Held`
amount reserved by active holds. **`Available = Balance - Held`** is what can
actually be spent. A direct transfer can only spend *available* funds.

**Idempotency keys.** Authorize, capture, and transfer all carry an idempotency
key. The first request applies; any retry with the same key returns the original
result without applying it again. This is what makes the system safe to retry.

## Authorization holds (the payment lifecycle)

A hold reserves funds before deciding to move them — the primitive behind card
auth-then-capture, wallet reserve-then-settle, and placing then settling a wager.

```
Authorize ──> hold ACTIVE (funds reserved, Available drops, nothing moved)
                │
                ├─ Capture(amount ≤ held) ─> money moves; remainder released ─> CAPTURED
                ├─ Void                    ─> all funds released, nothing moved ─> VOIDED
                └─ (deadline passes)       ─> ExpireHolds releases the funds   ─> EXPIRED
```

- **Capture** can be partial: capturing 100 of a 300 hold moves 100 and returns
  the other 200 to available.
- **Void** is idempotent (voiding twice never double-releases).
- **Expiry** is deterministic (the clock is injectable) so stale reservations
  don't fence off funds forever.

## Atomicity lives in the Store

Business validation (positive amount, distinct accounts, key required) lives in
the `Service`; atomic, idempotent application lives behind the `Store` interface.

- `MemStore` — an in-memory reference implementation. A single mutex makes every
  operation serializable, so it is the simplest possible *specification* of
  correctness, and the concurrency tests run against it.
- PostgreSQL (`internal/store/postgres`, schema in
  [`migrations/0001_init.sql`](migrations/0001_init.sql)) — a `UNIQUE` constraint
  on the idempotency key plus `SELECT ... FOR UPDATE` (in a deadlock-safe, sorted
  lock order) inside one transaction give the same guarantees at scale. A
  testcontainers conformance suite runs the same scenarios as the in-memory
  reference against a real Postgres.

## What the tests prove

Run with the race detector (`make race`):

- **Idempotency under retries** — 200 concurrent retries of one key apply a
  transfer exactly once.
- **No lost updates / no double-spend** — 500 concurrent transfers conserve the
  total balance exactly.
- **Property-based fuzzing** — `FuzzTransfers` throws millions of random transfer
  sequences at the ledger and asserts three invariants on every one: money is
  conserved, each balance equals opening + its entries, and all entries net to
  zero.
- **The hold lifecycle** — reserve-without-moving, full and partial capture,
  void (and idempotent double-void), expiry, and that a direct transfer can never
  spend held funds.
- Validation across the board: insufficient funds, currency mismatch, unknown
  account, over-capture, capture-after-void, expired-hold, and idempotency
  conflicts.

~90% statement coverage; `go vet` and `gofmt` clean.

## Layout

```
internal/ledger/
  ledger.go        domain types (accounts, entries, transfers, holds) + errors
  store.go         the persistence/atomicity boundary
  memstore.go      in-memory reference Store (concurrency-safe)
  service.go       business rules, validation, the injectable clock
  service_test.go  transfer + idempotency + concurrency proofs
  holds_test.go    the hold lifecycle proofs
  fuzz_test.go     property-based invariant fuzzing
  example_test.go  a runnable godoc example
internal/store/postgres/
  store.go / transfers.go / holds.go   the PostgreSQL Store (pgx, FOR UPDATE)
  conformance_test.go                  testcontainers suite (build tag: integration)
migrations/0001_init.sql   the PostgreSQL schema (accounts, transfers, entries, holds)
cmd/ledger/main.go         a tiny runnable demo
```

Integration tests are behind a build tag, so the default `go test ./...` needs no
database; run them with `go test -tags=integration ./...` (requires Docker).

## Roadmap

- [x] **M1** — double-entry core, idempotent transfers, concurrency- and fuzz-proven.
- [x] **Holds** — authorize / capture / void / expire, with available-balance semantics.
- [x] **M2** — PostgreSQL `Store` (`FOR UPDATE` + unique idempotency key) + testcontainers integration tests, with CI.
- [ ] **M3** — transactional outbox publishing `transfer.posted` events to Kafka.
- [ ] **M4** — gRPC + REST API, Prometheus metrics, Docker Compose.

## License

MIT — see [LICENSE](LICENSE).
