# Changelog

Notable changes to this module. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org) with v0.x semantics (minor versions may change
the API).

## [0.2.0] — 2026-07-13

### Added

- Multi-leg (n:n) postings as the general form of money movement: `Service.Post`
  applies a balanced set of two or more signed legs — a fee split, a settlement —
  atomically and idempotently. REST `POST /v1/postings` and gRPC `CreatePosting`
  expose it; the existing transfer endpoints are unchanged and now sugar over
  the two-leg case. Six new `ledgertest` scenarios (catalog v1 grows 20 → 26,
  add-only) cover fee splits, atomic multi-debit rejection, n-leg idempotent
  replay, and concurrent n-leg conservation.

- Idempotency-key retention and outbox pruning: `ledger_prune(key_retention,
  outbox_retention)` (migration 0005) NULLs idempotency keys older than the
  window — ledger rows are never deleted, and active holds keep their keys —
  and deletes published outbox rows past theirs (unpublished rows are never
  touched). The server gains an opt-in retention ticker: `PRUNE_INTERVAL`
  gates it, `KEY_RETENTION` (default 30 days) and `OUTBOX_RETENTION` (default
  7 days) size the windows. The retry contract is documented and pinned by an
  integration test: a retry arriving after its key was pruned is a new
  operation.

- Outbox observability: `ledger_outbox_published_total`,
  `ledger_outbox_unpublished`, and `ledger_outbox_lag_seconds` metrics, and a
  provisioned Grafana dashboard (money movements/s, latency p99, outbox
  backlog and lag) that appears on `docker compose up` with no clicking.

- Terminal captures in the README: the demo lifecycle and the load test ending
  on its conservation invariant.

### Changed

- **Breaking (v0.x):** the `ledger.Store` interface replaces
  `ApplyTransfer(TransferRequest)` with `ApplyPosting(PostRequest)` — both
  bundled stores implement only the general path, and a two-party transfer is
  simply the two-leg case. `Service.Transfer` keeps its signature and behavior.
- `transfers.from_account_id`, `to_account_id`, and `amount` are nullable as of
  migration 0004: populated for two-leg transfers (readability), NULL for
  larger postings — the entries are the record, all-or-nothing enforced by a
  CHECK constraint.

## [0.1.0] — 2026-07-13

First tagged release. Import paths are stable as of this tag: the core is
`github.com/mkmbhs/ledger` (module root), the PostgreSQL store is
`github.com/mkmbhs/ledger/postgres`, and the conformance suite is
`github.com/mkmbhs/ledger/ledgertest`.

### Added

- Double-entry core: accounts, balanced transfers, integer minor-unit money,
  and the full authorization-hold lifecycle (authorize → capture / void /
  expire), with idempotency keys on every money-moving operation.
- `MemStore`, the in-memory reference implementation that serves as the
  executable specification, and a PostgreSQL store (pgx, `SELECT ... FOR
  UPDATE` in sorted order, unique idempotency keys) that provides the same
  guarantees durably.
- `ledgertest`: a store-agnostic conformance suite (v1, 20 scenarios) any
  `ledger.Store` implementation can run. Both bundled stores pass it; the
  MemStore run is untagged and needs no external services.
- The zero-sum invariant enforced on every path that writes entries
  (`ledger.AssertBalanced`), plus a deferred PostgreSQL constraint trigger so
  the database itself refuses a commit whose entries do not balance.
- Idempotent account creation: identical re-create is a no-op, a mismatched
  re-create returns `ErrAccountExists` — an existing balance is never reset.
- Transactional outbox: `transfer.posted` events commit atomically with the
  money movement; an at-least-once relay (`FOR UPDATE SKIP LOCKED`) drains
  them to Kafka.
- REST and gRPC transports over one domain service, Prometheus metrics, a
  Docker Compose stack, a demo (`cmd/ledger`), an example consumer, and a
  load generator whose exit code asserts money conservation.
