-- Transactional outbox (M3). The Postgres Store writes a domain event into this
-- table inside the SAME transaction that moves the money, so the event and the
-- balance change commit together or not at all. This closes the dual-write gap of
-- "update the DB, then publish to Kafka" — two non-atomic steps where a crash in
-- between either loses the event or emits one for a change that rolled back.
--
-- A separate relay (internal/outbox) drains unpublished rows to the broker and
-- stamps published_at. Delivery is at-least-once: a crash between publishing and
-- stamping re-publishes the row, so consumers dedupe on the event id.

CREATE TABLE outbox (
    id           TEXT PRIMARY KEY,                 -- event id; the dedupe key for consumers
    event_type   TEXT NOT NULL,                    -- e.g. 'transfer.posted'
    aggregate_id TEXT NOT NULL,                    -- the entity the event is about (transfer id)
    payload      JSONB NOT NULL,                   -- the serialized event body
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ                        -- NULL until the relay ships it
);

-- The relay only ever scans the unpublished tail. A partial index keeps that scan
-- tiny — it indexes just the rows WHERE published_at IS NULL, ordered by arrival —
-- so the backlog query stays cheap even as the published history grows unbounded.
CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
