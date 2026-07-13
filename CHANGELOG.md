# Changelog

Notable changes to this module. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org) with v0.x semantics (minor versions may change
the API).

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
