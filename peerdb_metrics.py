#!/usr/bin/env python3
"""Collect canonical PeerDB/ClickHouse metrics for one large transaction."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SQL_ROOT = Path(__file__).with_name("sql") / "clickhouse"

METRIC_FIELDS = [
    "run_id",
    "transaction_rows",
    "commit_ts",
    "first_raw_ts",
    "last_raw_ts",
    "commit_to_first_raw_s",
    "raw_record_span_s",
    "peerdb_batch_count",
    "peerdb_batch_ids",
    "raw_insert_start_ts",
    "raw_insert_finish_ts",
    "raw_insert_ms",
    "raw_insert_memory_mib",
    "intermediate_processing_ms",
    "final_insert_start_ts",
    "final_insert_finish_ts",
    "final_insert_ms",
    "final_insert_memory_mib",
    "first_sync_ts",
    "last_sync_ts",
    "sync_timestamp_count",
    "commit_to_sync_s",
]


def quote_identifier(value: str) -> str:
    parts = value.split(".")
    if len(parts) not in (1, 2) or not all(IDENTIFIER.fullmatch(p) for p in parts):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return ".".join(f"`{part}`" for part in parts)


def table_fragment(value: str) -> str:
    return value.split(".")[-1]


def render_sql(filename: str, **identifiers: str) -> str:
    sql = (SQL_ROOT / filename).read_text(encoding="utf-8")
    for name, value in identifiers.items():
        sql = sql.replace("{{" + name + "}}", quote_identifier(value))
    unresolved = re.findall(r"\{\{([a-z_]+)\}\}", sql)
    if unresolved:
        raise ValueError(f"Unresolved SQL identifiers: {', '.join(unresolved)}")
    return sql


def classify_insert(query: str, raw_table: str, destination_table: str) -> str | None:
    """Classify final first because its SELECT commonly also names raw."""
    normalized = " ".join(query.lower().split())
    if not normalized.startswith("insert into"):
        return None
    destination = table_fragment(destination_table).lower()
    raw = table_fragment(raw_table).lower()
    if destination in normalized:
        return "final_insert"
    if raw in normalized:
        return "raw_insert"
    return None


def select_raw_table(matches: dict[str, int]) -> str:
    containing = [table for table, rows in matches.items() if rows > 0]
    if len(containing) == 1:
        return containing[0]
    if not containing:
        raise ValueError(
            "No PeerDB raw-table candidate contains the requested run_id; "
            "pass --raw-table after verifying the schema"
        )
    raise ValueError(
        "The run_id appears in multiple PeerDB raw tables; pass --raw-table: "
        + ", ".join(sorted(containing))
    )


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _one_named(result: Any) -> dict[str, Any]:
    rows = list(result.named_results())
    if len(rows) != 1:
        raise RuntimeError(f"Expected one result row, received {len(rows)}")
    return {key: _plain(value) for key, value in rows[0].items()}


def _parse_timestamp(value: str) -> datetime:
    normalized = value.strip().replace(" UTC", "+00:00")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)


def _query(
    client: Any, sql: str, parameters: dict[str, Any] | None = None
) -> dict[str, Any]:
    return _one_named(client.query(sql, parameters=parameters or {}))


def discover_raw_table(client: Any, run_id: str) -> str:
    candidates = [
        f"{database}.{name}"
        for database, name in client.query(
            render_sql("02_discover_peerdb_raw_table.sql")
        ).result_rows
    ]
    matches: dict[str, int] = {}
    for candidate in candidates:
        sql = (
            f"SELECT 1 FROM {quote_identifier(candidate)} "
            "WHERE JSONExtractString(_peerdb_data, 'run_id') = {run_id:String} "
            "LIMIT 1"
        )
        matches[candidate] = int(
            bool(client.query(sql, parameters={"run_id": run_id}).result_rows)
        )
    return select_raw_table(matches)


def collect_metrics(
    client: Any,
    *,
    run_id: str,
    commit_ts: str,
    destination_table: str,
    transaction_rows: int,
    raw_table: str | None = None,
    padding_before: float = 10,
    padding_after: float = 10,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    raw_table = raw_table or discover_raw_table(client, run_id)
    common = {"run_id": run_id, "commit_ts": commit_ts}
    destination = _query(
        client,
        render_sql("01_destination_summary.sql", destination_table=destination_table),
        common,
    )
    if not destination.get("rows"):
        raise RuntimeError(f"No destination rows found for run_id {run_id}")

    final_sync_ts = str(destination["last_synced_at"])
    raw = _query(
        client,
        render_sql("05_peerdb_raw_timing_summary.sql", raw_table=raw_table),
        {**common, "final_sync_ts": final_sync_ts},
    )
    if not raw.get("raw_records"):
        raise RuntimeError(f"No raw PeerDB rows found for run_id {run_id}")

    start = _parse_timestamp(commit_ts) - timedelta(seconds=padding_before)
    end = _parse_timestamp(final_sync_ts) + timedelta(seconds=padding_after)
    log_parameters = {
        "window_start": start.astimezone(timezone.utc).isoformat(),
        "window_end": end.astimezone(timezone.utc).isoformat(),
        "raw_table_fragment": table_fragment(raw_table),
        "destination_table_fragment": table_fragment(destination_table),
        "transaction_rows": transaction_rows,
    }
    operations = _query(
        client,
        render_sql("09_raw_to_final_gap.sql"),
        log_parameters,
    )
    metrics = {
        "schema_version": 1,
        "run_id": run_id,
        "transaction_rows": transaction_rows,
        "commit_ts": commit_ts,
        "destination_table": destination_table,
        "raw_peerdb_table": raw_table,
        "first_raw_ts": raw.get("first_raw_record_at"),
        "last_raw_ts": raw.get("last_raw_record_at"),
        "commit_to_first_raw_s": raw.get("commit_to_first_raw_seconds"),
        "raw_record_span_s": raw.get("raw_record_span_seconds"),
        "peerdb_batch_count": raw.get("batches"),
        "peerdb_batch_ids": raw.get("batch_ids") or [],
        "raw_insert_start_ts": operations.get("raw_insert_start"),
        "raw_insert_finish_ts": operations.get("raw_insert_finish"),
        "raw_insert_ms": operations.get("raw_insert_ms"),
        "raw_insert_memory_mib": operations.get("raw_insert_memory_mib"),
        "intermediate_processing_ms": operations.get("intermediate_processing_ms"),
        "final_insert_start_ts": operations.get("final_insert_start"),
        "final_insert_finish_ts": operations.get("final_insert_finish"),
        "final_insert_ms": operations.get("final_insert_ms"),
        "final_insert_memory_mib": operations.get("final_insert_memory_mib"),
        "first_sync_ts": destination.get("first_synced_at"),
        "last_sync_ts": destination.get("last_synced_at"),
        "sync_timestamp_count": destination.get("sync_timestamp_count"),
        "commit_to_sync_s": destination.get("commit_to_last_sync_seconds"),
        "measurement_scope": "one observed run",
    }
    events = metrics_to_events(metrics)
    return metrics, events


def metrics_to_events(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    mapping = [
        ("postgres_commit", "commit_ts"),
        ("first_raw_cdc", "first_raw_ts"),
        ("last_raw_cdc", "last_raw_ts"),
        ("raw_insert", "raw_insert_start_ts", "raw_insert_finish_ts"),
        ("final_insert", "final_insert_start_ts", "final_insert_finish_ts"),
        ("destination_sync", "last_sync_ts"),
    ]
    events: list[dict[str, Any]] = []
    for item in mapping:
        if len(item) == 2:
            stage, field = item
            if metrics.get(field) is not None:
                events.append({"stage": stage, "ts": metrics[field]})
        else:
            stage, start_field, finish_field = item
            events.append(
                {
                    "stage": stage,
                    "start_ts": metrics.get(start_field),
                    "finish_ts": metrics.get(finish_field),
                    "available": metrics.get(start_field) is not None
                    and metrics.get(finish_field) is not None,
                }
            )
    return events


def write_artifacts(
    output: Path, metrics: dict[str, Any], events: Iterable[dict[str, Any]]
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "metrics.json").write_text(
        json.dumps(metrics, indent=2, default=_plain) + "\n", encoding="utf-8"
    )
    with (output / "metrics.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=METRIC_FIELDS)
        writer.writeheader()
        row = {field: metrics.get(field) for field in METRIC_FIELDS}
        row["peerdb_batch_ids"] = json.dumps(row["peerdb_batch_ids"])
        writer.writerow(row)
    (output / "events.json").write_text(
        json.dumps(list(events), indent=2, default=_plain) + "\n", encoding="utf-8"
    )


def make_client() -> Any:
    import clickhouse_connect

    load_dotenv()
    required = [
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_DATABASE",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit("Missing environment variables: " + ", ".join(missing))
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        secure=os.getenv("CLICKHOUSE_SECURE", "true").lower() in {"1", "true", "yes"},
        query_limit=0,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit-ts", required=True)
    parser.add_argument("--destination-table", required=True)
    parser.add_argument("--raw-table")
    parser.add_argument("--transaction-rows", required=True, type=int)
    parser.add_argument("--query-log-padding-before", type=float, default=10)
    parser.add_argument("--query-log-padding-after", type=float, default=10)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or Path("results") / args.run_id
    client = make_client()
    try:
        metrics, events = collect_metrics(
            client,
            run_id=args.run_id,
            commit_ts=args.commit_ts,
            destination_table=args.destination_table,
            raw_table=args.raw_table,
            transaction_rows=args.transaction_rows,
            padding_before=args.query_log_padding_before,
            padding_after=args.query_log_padding_after,
        )
        write_artifacts(output, metrics, events)
    finally:
        client.close()
    print(f"Wrote canonical PeerDB metrics to {output}")


if __name__ == "__main__":
    main()
