-- Render {{destination_table}} as a validated identifier. Supply run_id and
-- commit_ts as ClickHouse query parameters.
WITH parseDateTime64BestEffort({commit_ts:String}, 6, 'UTC') AS commit_ts
SELECT
    count() AS rows,
    uniqExact(row_number) AS unique_rows,
    min(_peerdb_synced_at) AS first_synced_at,
    max(_peerdb_synced_at) AS last_synced_at,
    uniqExact(_peerdb_synced_at) AS sync_timestamp_count,
    dateDiff('microsecond', commit_ts, min(_peerdb_synced_at)) / 1000000.0
        AS commit_to_first_sync_seconds,
    dateDiff('microsecond', commit_ts, max(_peerdb_synced_at)) / 1000000.0
        AS commit_to_last_sync_seconds,
    dateDiff(
        'microsecond',
        min(_peerdb_synced_at),
        max(_peerdb_synced_at)
    ) / 1000000.0 AS destination_sync_timestamp_span_seconds
FROM {{destination_table}}
WHERE toString(run_id) = {run_id:String};
