# ledger

A small, correct double-entry ledger in Go, focused on the two things that make
money movement hard: **transactional correctness** and **safe retries**.

It is intentionally not a framework. It is a clear, tested core that demonstrates
how to move money without ever creating it, destroying it, or applying the same
request twice.

```
go test -race ./...     # the concurrency + idempotency proofs
go run ./cmd/ledger     # a 10-line demo
```

## The problem

Every payments, wallet, or settlement system has to answer the same questions:

- If a client retries a transfer after a timeout, does the money move twice?
- If two transfers touch the same account at the same time, can a balance be lost?
- Does every movement balance, so the books always reconcile?

This repo answers those three with the smallest amount of code that makes the
answers obvious and testable.

## Design decisions

**Double-entry.** Every transfer produces two `entries`: a debit on the source
and a credit on the destination, signed so they always sum to zero. Money is
never created or destroyed; the books always balance. `assertBalanced` enforces
this invariant on every transfer.

**Integer money.** Amounts are `int64` minor units (e.g. cents). Money is never a
float.

**Idempotency keys.** `Transfer` carries an `IdempotencyKey`. The first request
with a key applies the transfer; any later request with the same key returns the
original result without applying it again. Reusing a key with *different*
parameters is a client bug and is rejected (`ErrIdempotencyConflict`). This is
what makes the system safe to retry.

**Atomicity lives in the Store.** Business validation (positive amount, distinct
accounts) lives in the `Service`; the atomic, idempotent application lives behind
the `Store` interface. Two implementations:

- `MemStore` — an in-memory reference implementation. A single mutex makes every
  operation serializable, so it is the simplest possible *specification* of
  correctness. The concurrency tests run against it.
- PostgreSQL (see [`migrations/0001_init.sql`](migrations/0001_init.sql)) — a
  `UNIQUE` constraint on the idempotency key plus `SELECT ... FOR UPDATE` on the
  two accounts inside one transaction give the same guarantees at scale.
  *(Implementation is the next milestone — see Roadmap.)*

## What the tests prove

Run with the race detector (`go test -race ./...`):

- **Idempotency under retries** — 200 concurrent retries of one key apply the
  transfer exactly once (`TestTransfer_ConcurrentSameKey`).
- **No lost updates / no double-spend** — 500 concurrent transfers conserve the
  total balance exactly (`TestTransfer_ConcurrentConservation`).
- **The double-entry invariant** — every transfer's entries sum to zero.
- Validation: insufficient funds, currency mismatch, unknown account, non-positive
  amount, self-transfer, idempotency conflict.

## Layout

```
internal/ledger/
  ledger.go      domain types + errors
  store.go       the persistence/atomicity boundary
  memstore.go    in-memory reference Store (concurrency-safe)
  service.go     business rules + the double-entry invariant
  service_test.go  idempotency + concurrency proofs
migrations/0001_init.sql   the PostgreSQL schema
cmd/ledger/main.go         a tiny runnable demo
```

## Roadmap

- [x] **M1** — core ledger, idempotent transfers, concurrency-proven tests.
- [ ] **M2** — PostgreSQL `Store` (`FOR UPDATE` + unique idempotency key) + integration tests.
- [ ] **M3** — transactional outbox publishing `transfer.posted` events to Kafka.
- [ ] **M4** — gRPC + REST API, Prometheus metrics, Docker Compose, CI.

## License

MIT — see [LICENSE](LICENSE).
