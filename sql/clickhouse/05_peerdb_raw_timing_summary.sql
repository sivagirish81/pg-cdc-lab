-- Render {{raw_table}} as a validated identifier. Supply run_id, commit_ts,
-- and final_sync_ts as query parameters.
WITH
    parseDateTime64BestEffort({commit_ts:String}, 9, 'UTC') AS commit_ts,
    parseDateTime64BestEffort({final_sync_ts:String}, 9, 'UTC') AS final_sync_ts
SELECT
    count() AS raw_records,
    uniqExact(_peerdb_batch_id) AS batches,
    groupUniqArray(_peerdb_batch_id) AS batch_ids,
    fromUnixTimestamp64Nano(min(_peerdb_timestamp)) AS first_raw_record_at,
    fromUnixTimestamp64Nano(max(_peerdb_timestamp)) AS last_raw_record_at,
    dateDiff(
        'microsecond',
        commit_ts,
        fromUnixTimestamp64Nano(min(_peerdb_timestamp))
    ) / 1000000.0 AS commit_to_first_raw_seconds,
    dateDiff(
        'microsecond',
        fromUnixTimestamp64Nano(min(_peerdb_timestamp)),
        fromUnixTimestamp64Nano(max(_peerdb_timestamp))
    ) / 1000000.0 AS raw_record_span_seconds,
    dateDiff(
        'microsecond',
        fromUnixTimestamp64Nano(max(_peerdb_timestamp)),
        final_sync_ts
    ) / 1000000.0 AS last_raw_to_final_sync_seconds,
    dateDiff('microsecond', commit_ts, final_sync_ts) / 1000000.0
        AS total_commit_to_sync_seconds
FROM {{raw_table}}
WHERE JSONExtractString(_peerdb_data, 'run_id') = {run_id:String};
