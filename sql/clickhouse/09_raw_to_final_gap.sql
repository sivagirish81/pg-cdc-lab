-- Supply window_start/window_end, raw_table_fragment,
-- destination_table_fragment, and transaction_rows parameters.
-- Final is checked first because INSERT INTO final SELECT ... FROM raw contains
-- both table names. Nullable aggregates prevent fake 1970 epoch timestamps.
WITH ops AS
(
    SELECT
        query_start_time_microseconds AS start_ts,
        event_time_microseconds AS finish_ts,
        query_duration_ms,
        round(memory_usage / 1024 / 1024, 2) AS memory_mib,
        multiIf(
            query ILIKE concat(
                'INSERT INTO %',
                {destination_table_fragment:String},
                '%'
            ),
            'final_insert',
            query ILIKE concat('INSERT INTO %', {raw_table_fragment:String}, '%'),
            'raw_insert',
            'other'
        ) AS stage
    FROM clusterAllReplicas('default', system.query_log)
    WHERE event_time >= parseDateTimeBestEffort({window_start:String}, 'UTC')
      AND event_time <= parseDateTimeBestEffort({window_end:String}, 'UTC')
      AND type = 'QueryFinish'
      AND written_rows = {transaction_rows:UInt64}
      AND (
          query ILIKE concat('INSERT INTO %', {raw_table_fragment:String}, '%')
          OR query ILIKE concat(
              'INSERT INTO %',
              {destination_table_fragment:String},
              '%'
          )
      )
)
SELECT
    if(
        countIf(stage = 'raw_insert') = 0,
        NULL,
        minIf(start_ts, stage = 'raw_insert')
    ) AS raw_insert_start,
    if(
        countIf(stage = 'raw_insert') = 0,
        NULL,
        maxIf(finish_ts, stage = 'raw_insert')
    ) AS raw_insert_finish,
    if(
        countIf(stage = 'raw_insert') = 0,
        NULL,
        maxIf(query_duration_ms, stage = 'raw_insert')
    ) AS raw_insert_ms,
    if(
        countIf(stage = 'raw_insert') = 0,
        NULL,
        maxIf(memory_mib, stage = 'raw_insert')
    ) AS raw_insert_memory_mib,
    if(
        countIf(stage = 'final_insert') = 0,
        NULL,
        minIf(start_ts, stage = 'final_insert')
    ) AS final_insert_start,
    if(
        countIf(stage = 'final_insert') = 0,
        NULL,
        maxIf(finish_ts, stage = 'final_insert')
    ) AS final_insert_finish,
    if(
        countIf(stage = 'final_insert') = 0,
        NULL,
        maxIf(query_duration_ms, stage = 'final_insert')
    ) AS final_insert_ms,
    if(
        countIf(stage = 'final_insert') = 0,
        NULL,
        maxIf(memory_mib, stage = 'final_insert')
    ) AS final_insert_memory_mib,
    if(
        raw_insert_finish IS NULL OR final_insert_start IS NULL,
        NULL,
        dateDiff('microsecond', raw_insert_finish, final_insert_start) / 1000.0
    ) AS intermediate_processing_ms
FROM ops;
