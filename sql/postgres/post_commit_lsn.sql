\set ON_ERROR_STOP on

-- Run immediately after COMMIT when using the interactive manual workflow.
-- commit_observed_at is a client-requested post-COMMIT marker, not the exact
-- server-internal commit timestamp.
SELECT
    clock_timestamp() AS commit_observed_at,
    pg_current_wal_insert_lsn() AS post_commit_lsn;
