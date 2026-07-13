-- Idempotency-key retention and outbox pruning.
--
-- Keys and published outbox rows otherwise accumulate forever. The answer here
-- is deliberate and narrow:
--
--   * Ledger rows are immutable, forever. Nothing ever deletes transfers,
--     entries, or holds. Retention NULLs only the idempotency_key column of
--     old rows (UNIQUE permits any number of NULLs, and the key lookup
--     `WHERE idempotency_key = $1` never matches NULL), so history and
--     reconciliation are untouched.
--   * Published outbox rows are delivery bookkeeping, not ledger history —
--     they are deleted after their retention. Unpublished rows are NEVER
--     touched, no matter how old: undelivered events must survive until the
--     relay ships them.
--   * The key of an ACTIVE hold is never pruned. An active hold is a live
--     obligation, and its authorize must stay replayable until the hold
--     reaches a terminal state (captured / voided / expired).
--
-- THE CONTRACT (the honest edge every real payments ledger has): a retry that
-- arrives after its key was pruned is a NEW operation — a duplicate. Retention
-- must therefore exceed the longest possible client retry horizon; never retry
-- a request older than the retention window. The bundled server defaults to 30
-- days for keys (a conservative multiple of the field: Stripe expires keys
-- after 24h, Adyen after ~7 days) and 7 days for published outbox rows.

ALTER TABLE transfers ALTER COLUMN idempotency_key DROP NOT NULL;
ALTER TABLE holds     ALTER COLUMN idempotency_key DROP NOT NULL;

-- The prunable tails stay cheap to find even as history grows: partial indexes
-- cover exactly the rows a prune can still touch (same pattern as
-- outbox_unpublished_idx).
CREATE INDEX transfers_prunable_idx ON transfers (created_at) WHERE idempotency_key IS NOT NULL;
CREATE INDEX holds_prunable_idx     ON holds (created_at)     WHERE idempotency_key IS NOT NULL AND status <> 'active';
CREATE INDEX outbox_published_idx   ON outbox (published_at)  WHERE published_at IS NOT NULL;

-- ledger_prune applies the retention policy and reports what it did. Run it
-- from a scheduler (the bundled server has an opt-in ticker; PRUNE_INTERVAL
-- gates it) or by hand:
--
--   SELECT * FROM ledger_prune('30 days', '7 days');
CREATE FUNCTION ledger_prune(key_retention INTERVAL, outbox_retention INTERVAL)
RETURNS TABLE (transfer_keys_nulled BIGINT, hold_keys_nulled BIGINT, outbox_rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    t BIGINT;
    h BIGINT;
    o BIGINT;
BEGIN
    -- A zero or negative retention would strip idempotency protection from
    -- everything, instantly. That is never a policy, always a typo: refuse it.
    IF key_retention <= interval '0' OR outbox_retention <= interval '0' THEN
        RAISE EXCEPTION 'ledger_prune: retentions must be positive (got %, %)', key_retention, outbox_retention;
    END IF;

    UPDATE transfers SET idempotency_key = NULL
    WHERE idempotency_key IS NOT NULL AND created_at < now() - key_retention;
    GET DIAGNOSTICS t = ROW_COUNT;

    UPDATE holds SET idempotency_key = NULL
    WHERE idempotency_key IS NOT NULL
      AND status <> 'active'
      AND created_at < now() - key_retention;
    GET DIAGNOSTICS h = ROW_COUNT;

    DELETE FROM outbox
    WHERE published_at IS NOT NULL AND published_at < now() - outbox_retention;
    GET DIAGNOSTICS o = ROW_COUNT;

    RETURN QUERY SELECT t, h, o;
END;
$$;
