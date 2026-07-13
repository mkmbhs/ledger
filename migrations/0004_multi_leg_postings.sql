-- Multi-leg (n:n) postings. The general form of money movement is a balanced
-- set of N signed entries — a fee split, a settlement — and the entries table
-- (one row per leg, summing to zero per transfer, enforced by the 0003
-- trigger) has carried that shape since 0001. What changes here is only the
-- transfers table's two-leg summary: from_account_id / to_account_id / amount
-- become NULLable. They are populated for two-leg transfers (one debit, one
-- credit) so demo queries stay readable, and NULL for larger postings, where
-- the entries are the record.

ALTER TABLE transfers
    ALTER COLUMN from_account_id DROP NOT NULL,
    ALTER COLUMN to_account_id   DROP NOT NULL,
    ALTER COLUMN amount          DROP NOT NULL;

-- The summary is all-or-nothing: either a transfer carries the full two-leg
-- summary or none of it. (The existing amount_positive and distinct_accounts
-- CHECKs pass automatically on NULLs and keep binding the two-leg case.)
ALTER TABLE transfers
    ADD CONSTRAINT two_leg_summary_all_or_none CHECK (
        ((from_account_id IS NULL) = (to_account_id IS NULL))
        AND ((to_account_id IS NULL) = (amount IS NULL))
    );
