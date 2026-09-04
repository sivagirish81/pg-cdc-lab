\set ON_ERROR_STOP on
\timing on

-- Usage:
-- psql "$PG_DSN" -v run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')" \
--   -v transaction_rows=500000 -v payload_bytes=256 \
--   -f sql/postgres/large_transaction.sql
--
-- This file commits once. For an interactive pause before COMMIT, copy the
-- statements through INSERT into psql, start the slot sampler, then COMMIT.

BEGIN;

INSERT INTO cdc_lab.cdc_probe_large
    (run_id, outcome, row_number, payload)
SELECT
    :'run_id'::uuid,
    'commit',
    row_number,
    left(
        repeat(md5(:'run_id' || ':' || row_number::text), 64),
        :payload_bytes
    )
FROM generate_series(1, :transaction_rows) AS rows(row_number);

COMMIT;

-- This is a post-COMMIT observation marker, not an exact internal commit time.
SELECT
    clock_timestamp() AS commit_observed_at,
    pg_current_wal_insert_lsn() AS post_commit_lsn,
    :'run_id'::uuid AS run_id,
    :transaction_rows::bigint AS transaction_rows;
