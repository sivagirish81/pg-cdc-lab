-- Never silently select the first result. If multiple candidates exist,
-- verify the requested run_id in each or pass --raw-table to the collector.
SELECT
    database,
    name
FROM system.tables
WHERE name LIKE '%peerdb_raw%'
ORDER BY database, name;
