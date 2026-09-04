-- Render {{destination_table}} as a validated identifier. Supply run_id as a
-- query parameter. A shared timestamp is batch metadata, not proof of a
-- zero-duration physical INSERT.
SELECT
    _peerdb_synced_at,
    count() AS rows_at_timestamp
FROM {{destination_table}}
WHERE toString(run_id) = {run_id:String}
GROUP BY _peerdb_synced_at
ORDER BY _peerdb_synced_at;
