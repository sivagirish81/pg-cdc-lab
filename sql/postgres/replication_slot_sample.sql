\set ON_ERROR_STOP on

-- Pass -v slot_name=... to select one slot. Without it, all logical slots in
-- the current database are returned. Run repeatedly at 100-250 ms intervals.
SELECT
    clock_timestamp() AS observed_at,
    s.slot_name,
    s.active,
    s.restart_lsn,
    s.confirmed_flush_lsn,
    pg_current_wal_lsn() AS current_wal_lsn,
    pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn) AS retained_wal_bytes,
    pg_wal_lsn_diff(
        pg_current_wal_lsn(),
        s.confirmed_flush_lsn
    ) AS unconfirmed_wal_bytes,
    s.wal_status,
    s.safe_wal_size
FROM pg_replication_slots AS s
WHERE s.slot_type = 'logical'
  AND s.database = current_database()
  AND (NOT :{?slot_name} OR s.slot_name = :'slot_name')
ORDER BY s.slot_name;
