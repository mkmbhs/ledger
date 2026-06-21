-- Schema for the ledger. The PostgreSQL Store (M2) maps onto this directly.
--
-- The two guarantees the application relies on are enforced at the database level:
--   1. Idempotency  -> a UNIQUE constraint on transfers.idempotency_key. A retried
--      request collides on insert; the Store catches the conflict and returns the
--      original transfer instead of applying it again.
--   2. Atomicity + no lost updates -> each transfer runs in a single transaction
--      that does SELECT ... FOR UPDATE on the two accounts before changing
--      balances, so concurrent transfers touching the same account serialize.

CREATE TABLE accounts (
    id         TEXT PRIMARY KEY,
    currency   TEXT NOT NULL,
    balance    BIGINT NOT NULL DEFAULT 0,        -- minor units (e.g. cents)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT balance_non_negative CHECK (balance >= 0)
);

CREATE TABLE transfers (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,         -- makes retries safe
    from_account_id TEXT NOT NULL REFERENCES accounts(id),
    to_account_id   TEXT NOT NULL REFERENCES accounts(id),
    amount          BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT amount_positive CHECK (amount > 0),
    CONSTRAINT distinct_accounts CHECK (from_account_id <> to_account_id)
);

-- One row per side of a transfer. Amount is signed: negative = debit, positive
-- = credit. The two rows of a transfer always sum to zero (double-entry).
CREATE TABLE entries (
    id          TEXT PRIMARY KEY,
    transfer_id TEXT NOT NULL REFERENCES transfers(id),
    account_id  TEXT NOT NULL REFERENCES accounts(id),
    amount      BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX entries_account_idx  ON entries(account_id);
CREATE INDEX entries_transfer_idx ON entries(transfer_id);
