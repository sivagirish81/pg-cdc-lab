-- Supply window_start/window_end and table_name_fragment parameters.
-- clusterAllReplicas is required because a Cloud SQL session can land on a
-- replica other than the one that executed the CDC queries.
SELECT
    hostname() AS host,
    query_start_time_microseconds,
    event_time_microseconds AS query_finish_time,
    query_duration_ms,
    read_rows,
    written_rows,
    result_rows,
    round(memory_usage / 1024 / 1024, 2) AS memory_mib,
    query
FROM clusterAllReplicas('default', system.query_log)
WHERE event_time >= parseDateTimeBestEffort({window_start:String}, 'UTC')
  AND event_time <= parseDateTimeBestEffort({window_end:String}, 'UTC')
  AND type = 'QueryFinish'
  AND query ILIKE concat('%', {table_name_fragment:String}, '%')
ORDER BY query_start_time_microseconds;
