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
    balance    BIGINT NOT NULL DEFAULT 0,        -- settled funds, minor units (e.g. cents)
    held       BIGINT NOT NULL DEFAULT 0,        -- reserved by active holds
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- available = balance - held; a hold can never reserve more than is settled.
    CONSTRAINT balance_non_negative CHECK (balance >= 0),
    CONSTRAINT held_within_balance  CHECK (held >= 0 AND held <= balance)
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

-- An authorization hold reserves funds (raising accounts.held) without moving
-- them, until it is captured (settled into a transfer) or released.
CREATE TABLE holds (
    id                  TEXT PRIMARY KEY,
    idempotency_key     TEXT NOT NULL UNIQUE,
    from_account_id     TEXT NOT NULL REFERENCES accounts(id),
    to_account_id       TEXT NOT NULL REFERENCES accounts(id),
    amount              BIGINT NOT NULL,                 -- reserved
    captured            BIGINT NOT NULL DEFAULT 0,       -- settled (<= amount)
    status              TEXT NOT NULL,                   -- active | captured | voided | expired
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ,                     -- NULL = no expiry
    capture_transfer_id TEXT REFERENCES transfers(id),   -- set once captured
    CONSTRAINT hold_amount_positive CHECK (amount > 0),
    CONSTRAINT hold_captured_valid  CHECK (captured >= 0 AND captured <= amount)
);

CREATE INDEX holds_active_expiry_idx ON holds(expires_at) WHERE status = 'active';
