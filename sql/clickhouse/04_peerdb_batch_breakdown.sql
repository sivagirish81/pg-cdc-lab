-- Render {{raw_table}} as a validated identifier. Supply run_id as a query
-- parameter.
SELECT
    _peerdb_batch_id,
    count() AS rows,
    min(_peerdb_timestamp) AS min_raw_timestamp,
    max(_peerdb_timestamp) AS max_raw_timestamp,
    fromUnixTimestamp64Nano(min(_peerdb_timestamp)) AS first_raw_record_at,
    fromUnixTimestamp64Nano(max(_peerdb_timestamp)) AS last_raw_record_at,
    dateDiff(
        'microsecond',
        fromUnixTimestamp64Nano(min(_peerdb_timestamp)),
        fromUnixTimestamp64Nano(max(_peerdb_timestamp))
    ) / 1000000.0 AS raw_record_span_seconds
FROM {{raw_table}}
WHERE JSONExtractString(_peerdb_data, 'run_id') = {run_id:String}
GROUP BY _peerdb_batch_id
ORDER BY first_raw_record_at;
