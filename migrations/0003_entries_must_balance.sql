-- Database-level backstop for the double-entry invariant: the entries of a
-- transfer must sum to zero. The application already refuses unbalanced writes
-- (ledger.AssertBalanced runs on every path that writes entries), but a check
-- the database enforces holds even against a future application bug: the
-- database will not let the books go out of balance.
--
-- The trigger is DEFERRED to commit time because a transfer's entries are
-- inserted as separate rows within one transaction; mid-transaction the sum is
-- legitimately non-zero, so only the state at COMMIT is meaningful.
--
-- Scope: this enforces balance, not row immutability — deleting BOTH entries of
-- a transfer sums to zero and passes. Immutability is an application rule
-- (nothing ever deletes ledger rows); the backstop guarantees that whatever
-- rows exist always net to zero per transfer.

-- Refuse to install the backstop over already-corrupt data: if any existing
-- transfer is unbalanced, fail the migration loudly instead of hiding it.
DO $$
DECLARE
    bad RECORD;
BEGIN
    SELECT transfer_id, SUM(amount) AS total
    INTO bad
    FROM entries
    GROUP BY transfer_id
    HAVING SUM(amount) <> 0
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'ledger: existing entries for transfer % sum to %, not zero', bad.transfer_id, bad.total;
    END IF;
END;
$$;

-- Re-checks every transfer touched by the current transaction. UPDATE checks
-- both the old and new transfer_id so re-pointing an entry cannot unbalance
-- either side; DELETE checks what remains of the old transfer.
CREATE FUNCTION entries_must_balance() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    total BIGINT;
BEGIN
    IF TG_OP <> 'DELETE' THEN
        SELECT COALESCE(SUM(amount), 0) INTO total FROM entries WHERE transfer_id = NEW.transfer_id;
        IF total <> 0 THEN
            RAISE EXCEPTION 'ledger: entries for transfer % sum to %, not zero', NEW.transfer_id, total
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        SELECT COALESCE(SUM(amount), 0) INTO total FROM entries WHERE transfer_id = OLD.transfer_id;
        IF total <> 0 THEN
            RAISE EXCEPTION 'ledger: entries for transfer % sum to %, not zero', OLD.transfer_id, total
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER entries_must_balance
    AFTER INSERT OR UPDATE OR DELETE ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION entries_must_balance();
