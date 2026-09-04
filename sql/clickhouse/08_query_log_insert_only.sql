-- Supply window_start/window_end, raw_table_fragment, and
-- destination_table_fragment parameters.
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
  AND written_rows > 0
  AND (
      query ILIKE concat('INSERT INTO %', {raw_table_fragment:String}, '%')
      OR query ILIKE concat(
          'INSERT INTO %',
          {destination_table_fragment:String},
          '%'
      )
  )
ORDER BY query_start_time_microseconds;
