#!/usr/bin/env python3
"""pg-cdc-lab PostgreSQL transaction-boundary CDC benchmark."""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from clickpipe_metrics import (
    CSV_FIELDS as CLICKPIPE_METRICS_FIELDS,
    CloudMetricsSettings,
    scrape_clickpipe_metrics,
)

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LAB_METRICS: Any | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def validate_table_name(value: str) -> str:
    parts = value.split(".")
    if len(parts) not in (1, 2) or not all(
        IDENTIFIER.fullmatch(part) for part in parts
    ):
        raise ValueError(f"Unsafe table name: {value!r}")
    return ".".join(f'"{part}"' for part in parts)


def ch_table_name(value: str) -> str:
    parts = value.split(".")
    if len(parts) not in (1, 2) or not all(
        IDENTIFIER.fullmatch(part) for part in parts
    ):
        raise ValueError(f"Unsafe ClickHouse table name: {value!r}")
    return ".".join(f"`{part}`" for part in parts)


@dataclass(frozen=True)
class Config:
    pg_dsn: str
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    ch_database: str
    ch_secure: bool
    ch_small_table: str
    ch_large_table: str
    slot_name: str | None
    configuration: str
    sync_interval_seconds: float | None
    pull_batch_size: int | None
    cloud_organization_id: str | None
    cloud_api_key_id: str | None
    cloud_api_key_secret: str | None
    clickpipe_id: str | None
    clickpipe_metrics_poll_seconds: float

    @classmethod
    def from_env(cls, require_clickhouse: bool = True) -> "Config":
        from dotenv import load_dotenv

        load_dotenv()
        required = ["PG_DSN"]
        if require_clickhouse:
            required += [
                "CLICKHOUSE_HOST",
                "CLICKHOUSE_USER",
                "CLICKHOUSE_PASSWORD",
                "CLICKHOUSE_DATABASE",
                "CLICKHOUSE_SMALL_TABLE",
                "CLICKHOUSE_LARGE_TABLE",
            ]
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
        cloud_credentials = {
            "CLICKHOUSE_CLOUD_ORGANIZATION_ID": os.getenv(
                "CLICKHOUSE_CLOUD_ORGANIZATION_ID"
            ),
            "CLICKHOUSE_CLOUD_API_KEY_ID": os.getenv("CLICKHOUSE_CLOUD_API_KEY_ID"),
            "CLICKHOUSE_CLOUD_API_KEY_SECRET": os.getenv(
                "CLICKHOUSE_CLOUD_API_KEY_SECRET"
            ),
        }
        if any(cloud_credentials.values()) and not all(cloud_credentials.values()):
            missing_cloud = [
                name for name, value in cloud_credentials.items() if not value
            ]
            raise SystemExit(
                "Incomplete ClickHouse Cloud metrics configuration; missing: "
                + ", ".join(missing_cloud)
            )
        return cls(
            pg_dsn=os.environ["PG_DSN"],
            ch_host=os.getenv("CLICKHOUSE_HOST", ""),
            ch_port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
            ch_user=os.getenv("CLICKHOUSE_USER", ""),
            ch_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            ch_database=os.getenv("CLICKHOUSE_DATABASE", ""),
            ch_secure=os.getenv("CLICKHOUSE_SECURE", "true").lower()
            in {"1", "true", "yes"},
            ch_small_table=os.getenv("CLICKHOUSE_SMALL_TABLE", ""),
            ch_large_table=os.getenv("CLICKHOUSE_LARGE_TABLE", ""),
            slot_name=os.getenv("CLICKPIPE_SLOT") or None,
            configuration=os.getenv("PG_CDC_LAB_CONFIGURATION", "default_60s"),
            sync_interval_seconds=(
                float(os.environ["PG_CDC_LAB_SYNC_INTERVAL_SECONDS"])
                if os.getenv("PG_CDC_LAB_SYNC_INTERVAL_SECONDS")
                else None
            ),
            pull_batch_size=(
                int(os.environ["PG_CDC_LAB_PULL_BATCH_SIZE"])
                if os.getenv("PG_CDC_LAB_PULL_BATCH_SIZE")
                else None
            ),
            cloud_organization_id=cloud_credentials["CLICKHOUSE_CLOUD_ORGANIZATION_ID"],
            cloud_api_key_id=cloud_credentials["CLICKHOUSE_CLOUD_API_KEY_ID"],
            cloud_api_key_secret=cloud_credentials["CLICKHOUSE_CLOUD_API_KEY_SECRET"],
            clickpipe_id=os.getenv("CLICKPIPE_ID") or None,
            clickpipe_metrics_poll_seconds=float(
                os.getenv("CLICKPIPE_METRICS_POLL_SECONDS", "15")
            ),
        )

    def cloud_metrics_settings(self) -> CloudMetricsSettings | None:
        if not (
            self.cloud_organization_id
            and self.cloud_api_key_id
            and self.cloud_api_key_secret
        ):
            return None
        return CloudMetricsSettings(
            organization_id=self.cloud_organization_id,
            api_key_id=self.cloud_api_key_id,
            api_key_secret=self.cloud_api_key_secret,
            clickpipe_id=self.clickpipe_id,
        )


class CsvFile:
    def __init__(self, path: Path, fields: list[str]):
        self.path = path
        self.fields = fields
        self.lock = threading.Lock()
        self.handle = path.open("w", newline="", encoding="utf-8", buffering=1)
        self.writer = csv.DictWriter(
            self.handle, fieldnames=fields, extrasaction="ignore"
        )
        self.writer.writeheader()

    def write(self, row: dict[str, Any]) -> None:
        with self.lock:
            self.writer.writerow(row)

    def close(self) -> None:
        with self.lock:
            self.handle.close()


class Evidence:
    def __init__(self, result_dir: Path):
        self.result_dir = result_dir
        self.started_mono = time.perf_counter()
        self.phase = "warmup"
        self.event_lock = threading.Lock()
        self.event_file = (result_dir / "events.jsonl").open(
            "w", encoding="utf-8", buffering=1
        )
        self.errors_file = (result_dir / "errors.jsonl").open(
            "w", encoding="utf-8", buffering=1
        )
        self.commits = CsvFile(
            result_dir / "small_commits.csv",
            [
                "run_id",
                "worker_id",
                "seq",
                "scheduled_elapsed_s",
                "commit_ack_elapsed_s",
                "commit_latency_ms",
                "queue_delay_ms",
                "commit_ack_utc",
                "phase",
            ],
        )
        self.visibility = CsvFile(
            result_dir / "small_visibility.csv",
            [
                "run_id",
                "worker_id",
                "seq",
                "seen_elapsed_s",
                "seen_utc",
                "query_duration_ms",
                "phase",
            ],
        )
        self.slots = CsvFile(
            result_dir / "slot_samples.csv",
            [
                "sampled_at",
                "elapsed_s",
                "slot_name",
                "active",
                "restart_lsn",
                "confirmed_flush_lsn",
                "current_wal_lsn",
                "retained_wal_bytes",
                "unconfirmed_wal_bytes",
                "restart_to_confirmed_bytes",
                "wal_status",
                "safe_wal_size",
                "spill_txns",
                "spill_count",
                "spill_bytes",
                "stream_txns",
                "stream_count",
                "stream_bytes",
                "total_txns",
                "total_bytes",
                "phase",
            ],
        )
        self.large = CsvFile(
            result_dir / "large_visibility.csv",
            [
                "sampled_at",
                "elapsed_s",
                "run_id",
                "row_count",
                "unique_rows",
                "min_row_number",
                "max_row_number",
                "query_duration_ms",
                "phase",
            ],
        )
        self.small_integrity = CsvFile(
            result_dir / "small_integrity.csv",
            [
                "sampled_at",
                "elapsed_s",
                "run_id",
                "row_count",
                "unique_rows",
                "query_duration_ms",
                "phase",
            ],
        )
        self.clickpipe_metrics = CsvFile(
            result_dir / "clickpipe_metrics.csv", CLICKPIPE_METRICS_FIELDS
        )

    def elapsed(self) -> float:
        return time.perf_counter() - self.started_mono

    def event(self, name: str, **fields: Any) -> None:
        row = {
            "event": name,
            "utc": utc_now(),
            "elapsed_s": self.elapsed(),
            "phase": self.phase,
            **fields,
        }
        with self.event_lock:
            self.event_file.write(json.dumps(row, sort_keys=True) + "\n")

    def set_phase(self, phase: str) -> None:
        self.phase = phase
        self.event("phase_transition", phase=phase)
        if LAB_METRICS is not None:
            LAB_METRICS.set_phase(phase)

    def error(self, component: str, error: BaseException) -> None:
        row = {
            "component": component,
            "utc": utc_now(),
            "elapsed_s": self.elapsed(),
            "error_type": type(error).__name__,
            "message": re.sub(
                r"([a-z][a-z0-9+.-]*://)[^/@\s]+@",
                r"\1<redacted>@",
                str(error),
                flags=re.I,
            ),
            "phase": self.phase,
        }
        with self.event_lock:
            self.errors_file.write(json.dumps(row, sort_keys=True) + "\n")

    def close(self) -> None:
        self.event_file.close()
        self.errors_file.close()
        self.commits.close()
        self.visibility.close()
        self.slots.close()
        self.large.close()
        self.small_integrity.close()
        self.clickpipe_metrics.close()


class CommitRegistry:
    def __init__(self, workers: int):
        self.lock = asyncio.Lock()
        self.commits: dict[tuple[int, int], float] = {}
        self.visible: set[tuple[int, int]] = set()
        self.highwater = {worker: 0 for worker in range(workers)}

    async def add(self, worker: int, seq: int, commit_elapsed: float) -> None:
        async with self.lock:
            self.commits[(worker, seq)] = commit_elapsed

    async def snapshot_highwater(self) -> dict[int, int]:
        async with self.lock:
            return dict(self.highwater)

    async def mark_visible(self, worker: int, seq: int) -> bool:
        async with self.lock:
            key = (worker, seq)
            if key not in self.commits or key in self.visible:
                return False
            self.visible.add(key)
            # Each writer commits serially, so visibility within a writer is ordered.
            self.highwater[worker] = max(self.highwater[worker], seq)
            return True

    async def counts(self) -> tuple[int, int]:
        async with self.lock:
            return len(self.commits), len(self.visible)


def payload_for(run_id: uuid.UUID, number: int, size: int) -> str:
    digest = hashlib.sha256(f"{run_id}:{number}".encode()).hexdigest()
    repeats = (size + len(digest) - 1) // len(digest)
    return (digest * repeats)[:size]


async def import_pg():
    import psycopg
    from psycopg_pool import AsyncConnectionPool

    return psycopg, AsyncConnectionPool


def make_destination(config: Config):
    from destination import ClickHouseDestination, ClickHouseSettings

    return ClickHouseDestination(
        ClickHouseSettings(
            host=config.ch_host,
            port=config.ch_port,
            username=config.ch_user,
            password=config.ch_password,
            database=config.ch_database,
            secure=config.ch_secure,
            small_table=config.ch_small_table,
            large_table=config.ch_large_table,
        )
    )


async def setup_source(config: Config) -> None:
    psycopg, _ = await import_pg()
    sql = (Path(__file__).with_name("setup.sql")).read_text(encoding="utf-8")
    async with await psycopg.AsyncConnection.connect(
        config.pg_dsn, autocommit=True
    ) as conn:
        await conn.execute(sql)
    print("Created/verified cdc_lab.cdc_probe_small and cdc_lab.cdc_probe_large")


async def source_preflight(config: Config) -> dict[str, Any]:
    """Characterize PostgreSQL before a ClickPipe/logical slot necessarily exists."""
    psycopg, _ = await import_pg()
    async with await psycopg.AsyncConnection.connect(
        config.pg_dsn, autocommit=True
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT current_setting('server_version'),
                       current_setting('logical_decoding_work_mem', true),
                       current_setting('max_slot_wal_keep_size', true),
                       current_setting('wal_level', true),
                       current_database(), current_user, clock_timestamp()
                """
            )
            (
                version,
                work_mem,
                max_keep,
                wal_level,
                database,
                user,
                now,
            ) = await cur.fetchone()
            await cur.execute(
                """
                SELECT slot_name, plugin, active, restart_lsn::text,
                       confirmed_flush_lsn::text, wal_status, safe_wal_size
                FROM pg_replication_slots
                WHERE slot_type = 'logical' AND database = current_database()
                ORDER BY slot_name
                """
            )
            slots = await cur.fetchall()
            await cur.execute(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'cdc_lab'
                  AND c.relname IN ('cdc_probe_small', 'cdc_probe_large')
                ORDER BY c.relname
                """
            )
            source_tables = [f"{row[0]}.{row[1]}" for row in await cur.fetchall()]
    return {
        "server_version": version,
        "logical_decoding_work_mem": work_mem,
        "max_slot_wal_keep_size": max_keep,
        "wal_level": wal_level,
        "database": database,
        "user": user,
        "postgres_time": now.isoformat(),
        "logical_slots": [
            {
                "slot_name": row[0],
                "plugin": row[1],
                "active": row[2],
                "restart_lsn": row[3],
                "confirmed_flush_lsn": row[4],
                "wal_status": row[5],
                "safe_wal_size": row[6],
            }
            for row in slots
        ],
        "source_tables": source_tables,
        "credentials_redacted": True,
    }


async def select_slot(conn: Any, configured: str | None) -> str:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT slot_name
            FROM pg_replication_slots
            WHERE slot_type = 'logical' AND database = current_database()
            ORDER BY slot_name
            """
        )
        slots = [row[0] for row in await cur.fetchall()]
    if configured:
        if configured not in slots:
            raise RuntimeError(
                f"Configured slot {configured!r} not found; found {slots}"
            )
        return configured
    if len(slots) != 1:
        raise RuntimeError(
            "Set CLICKPIPE_SLOT because the current database has "
            f"{len(slots)} logical slots: {slots}"
        )
    return slots[0]


async def pg_preflight(config: Config) -> dict[str, Any]:
    psycopg, _ = await import_pg()
    async with await psycopg.AsyncConnection.connect(
        config.pg_dsn, autocommit=True
    ) as conn:
        slot = await select_slot(conn, config.slot_name)
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT current_setting('server_version'),
                       current_setting('logical_decoding_work_mem', true),
                       current_setting('max_slot_wal_keep_size', true),
                       current_database(), clock_timestamp()
                """
            )
            version, work_mem, max_keep, database, now = await cur.fetchone()
            await cur.execute(
                """
                SELECT slot_name, plugin, active, restart_lsn::text,
                       confirmed_flush_lsn::text, wal_status, safe_wal_size
                FROM pg_replication_slots WHERE slot_name = %s
                """,
                (slot,),
            )
            slot_row = await cur.fetchone()
        return {
            "server_version": version,
            "logical_decoding_work_mem": work_mem,
            "max_slot_wal_keep_size": max_keep,
            "database": database,
            "postgres_time": now.isoformat(),
            "slot": {
                "slot_name": slot_row[0],
                "plugin": slot_row[1],
                "active": slot_row[2],
                "restart_lsn": slot_row[3],
                "confirmed_flush_lsn": slot_row[4],
                "wal_status": slot_row[5],
                "safe_wal_size": slot_row[6],
            },
        }


def ch_preflight(config: Config) -> dict[str, Any]:
    destination = make_destination(config)
    try:
        return destination.preflight()
    finally:
        destination.close()


async def sample_small_integrity(
    config: Config, run_id: uuid.UUID, evidence: Evidence
) -> None:
    destination = await asyncio.to_thread(make_destination, config)
    started = time.perf_counter()
    try:
        row = await asyncio.to_thread(destination.small_integrity, str(run_id))
        evidence.small_integrity.write(
            {
                "sampled_at": utc_now(),
                "elapsed_s": evidence.elapsed(),
                "run_id": str(run_id),
                "row_count": int(row[0]),
                "unique_rows": int(row[1]),
                "query_duration_ms": (time.perf_counter() - started) * 1000,
                "phase": evidence.phase,
            }
        )
    except Exception as exc:
        evidence.error("clickhouse_small_integrity", exc)
    finally:
        await asyncio.to_thread(destination.close)


async def slot_monitor(
    config: Config,
    slot: str,
    evidence: Evidence,
    stop: asyncio.Event,
    interval: float,
    safe_wal_warning_bytes: int,
) -> None:
    psycopg, _ = await import_pg()
    query = """
        SELECT clock_timestamp(), s.slot_name, s.active, s.restart_lsn::text,
               s.confirmed_flush_lsn::text, pg_current_wal_lsn()::text,
               pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
               pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint,
               pg_wal_lsn_diff(s.confirmed_flush_lsn, s.restart_lsn)::bigint,
               s.wal_status, s.safe_wal_size,
               (to_jsonb(st)->>'spill_txns')::bigint,
               (to_jsonb(st)->>'spill_count')::bigint,
               (to_jsonb(st)->>'spill_bytes')::bigint,
               (to_jsonb(st)->>'stream_txns')::bigint,
               (to_jsonb(st)->>'stream_count')::bigint,
               (to_jsonb(st)->>'stream_bytes')::bigint,
               (to_jsonb(st)->>'total_txns')::bigint,
               (to_jsonb(st)->>'total_bytes')::bigint
        FROM pg_replication_slots s
        LEFT JOIN pg_stat_replication_slots st USING (slot_name)
        WHERE s.slot_name = %s
    """
    fields = evidence.slots.fields
    try:
        async with await psycopg.AsyncConnection.connect(
            config.pg_dsn, autocommit=True
        ) as conn:
            while not stop.is_set():
                async with conn.cursor() as cur:
                    await cur.execute(query, (slot,))
                    row = await cur.fetchone()
                if row:
                    values = list(row)
                    values[0] = values[0].isoformat()
                    record = dict(
                        zip(
                            [
                                field
                                for field in fields
                                if field not in {"elapsed_s", "phase"}
                            ],
                            values,
                        )
                    )
                    record["elapsed_s"] = evidence.elapsed()
                    record["phase"] = evidence.phase
                    evidence.slots.write(record)
                    if LAB_METRICS is not None:
                        labels = LAB_METRICS.labels
                        for metric, field in (
                            (LAB_METRICS.retained_wal, "retained_wal_bytes"),
                            (LAB_METRICS.unconfirmed_wal, "unconfirmed_wal_bytes"),
                            (
                                LAB_METRICS.restart_to_confirmed,
                                "restart_to_confirmed_bytes",
                            ),
                            (LAB_METRICS.spill_bytes, "spill_bytes"),
                            (LAB_METRICS.spill_txns, "spill_txns"),
                            (LAB_METRICS.stream_bytes, "stream_bytes"),
                            (LAB_METRICS.stream_txns, "stream_txns"),
                        ):
                            if record.get(field) is not None:
                                metric.labels(**labels).set(float(record[field]))
                    if record.get("wal_status") in {"unreserved", "lost"}:
                        evidence.event(
                            "unsafe_slot_status", wal_status=record["wal_status"]
                        )
                        stop.set()
                    safe_size = record.get("safe_wal_size")
                    if (
                        safe_wal_warning_bytes
                        and safe_size is not None
                        and int(safe_size) <= safe_wal_warning_bytes
                    ):
                        evidence.event(
                            "safe_wal_size_warning",
                            safe_wal_size=int(safe_size),
                            threshold=safe_wal_warning_bytes,
                        )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
    except Exception as exc:
        evidence.error("slot_monitor", exc)
        stop.set()


async def clickpipe_metrics_monitor(
    settings: CloudMetricsSettings,
    run_id: uuid.UUID,
    evidence: Evidence,
    stop: asyncio.Event,
    interval: float,
) -> None:
    failures = 0
    while not stop.is_set():
        try:
            record = await asyncio.to_thread(scrape_clickpipe_metrics, settings)
            record.update(
                {
                    "run_id": str(run_id),
                    "sampled_at": utc_now(),
                    "elapsed_s": evidence.elapsed(),
                    "phase": evidence.phase,
                }
            )
            evidence.clickpipe_metrics.write(record)
            if failures:
                evidence.event(
                    "clickpipe_metrics_scrape_recovered",
                    consecutive_failures=failures,
                )
            failures = 0
        except Exception as exc:
            failures += 1
            if failures == 1 or failures % 10 == 0:
                evidence.event(
                    "clickpipe_metrics_scrape_failed",
                    consecutive_failures=failures,
                    error_type=type(exc).__name__,
                    message=str(exc),
                )
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(interval, 1.0))
        except asyncio.TimeoutError:
            pass


async def small_worker(
    worker_id: int,
    config: Config,
    run_id: uuid.UUID,
    payload_size: int,
    queue: asyncio.Queue[tuple[int, float] | None],
    registry: CommitRegistry,
    evidence: Evidence,
) -> None:
    psycopg, _ = await import_pg()
    table = validate_table_name("cdc_lab.cdc_probe_small")
    seq = 0
    async with await psycopg.AsyncConnection.connect(config.pg_dsn) as conn:
        while True:
            job = await queue.get()
            if job is None:
                queue.task_done()
                return
            _, scheduled = job
            seq += 1
            started = evidence.elapsed()
            try:
                async with conn.transaction():
                    await conn.execute(
                        f"INSERT INTO {table} "
                        "(run_id, worker_id, seq, source_ts, payload) "
                        "VALUES (%s, %s, %s, clock_timestamp(), %s)",
                        (
                            run_id,
                            worker_id,
                            seq,
                            payload_for(run_id, worker_id * 10**12 + seq, payload_size),
                        ),
                    )
                committed = evidence.elapsed()
                await registry.add(worker_id, seq, committed)
                evidence.commits.write(
                    {
                        "run_id": str(run_id),
                        "worker_id": worker_id,
                        "seq": seq,
                        "scheduled_elapsed_s": scheduled,
                        "commit_ack_elapsed_s": committed,
                        "commit_latency_ms": (committed - started) * 1000,
                        "queue_delay_ms": max(0.0, started - scheduled) * 1000,
                        "commit_ack_utc": utc_now(),
                        "phase": evidence.phase,
                    }
                )
                if LAB_METRICS is not None:
                    labels = LAB_METRICS.phase_labels(evidence.phase)
                    LAB_METRICS.source_commit.labels(**labels).observe(
                        committed - started
                    )
                    LAB_METRICS.queue_delay.labels(**labels).observe(
                        max(0.0, started - scheduled)
                    )
                    LAB_METRICS.committed.labels(**labels).inc()
                    LAB_METRICS.achieved_tps.labels(**LAB_METRICS.labels).set(
                        len(registry.commits) / max(committed, 0.001)
                    )
            except Exception as exc:
                evidence.error(f"small_worker_{worker_id}", exc)
                try:
                    await conn.rollback()
                except Exception:
                    pass
            finally:
                queue.task_done()


async def small_producer(
    rate: float,
    queue: asyncio.Queue[tuple[int, float] | None],
    evidence: Evidence,
    stop: asyncio.Event,
) -> None:
    index = 0
    next_time = evidence.elapsed()
    while not stop.is_set():
        index += 1
        next_time += 1.0 / rate
        delay = next_time - evidence.elapsed()
        if delay > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                break
            except asyncio.TimeoutError:
                pass
        await queue.put((index, next_time))


async def clickhouse_observer(
    config: Config,
    run_id: uuid.UUID,
    registry: CommitRegistry,
    evidence: Evidence,
    stop: asyncio.Event,
    interval: float,
) -> None:
    destination = await asyncio.to_thread(make_destination, config)
    try:
        while not stop.is_set():
            highwater = await registry.snapshot_highwater()
            query_started = time.perf_counter()
            try:
                rows = await asyncio.to_thread(
                    destination.small_rows_after, str(run_id), highwater
                )
                seen_elapsed = evidence.elapsed()
                seen_utc = utc_now()
                query_duration_ms = (time.perf_counter() - query_started) * 1000
                if LAB_METRICS is not None:
                    LAB_METRICS.observer_query.labels(
                        **LAB_METRICS.phase_labels(evidence.phase)
                    ).observe(query_duration_ms / 1000)
                for worker, seq in rows:
                    if await registry.mark_visible(int(worker), int(seq)):
                        commit_elapsed = registry.commits[(int(worker), int(seq))]
                        evidence.visibility.write(
                            {
                                "run_id": str(run_id),
                                "worker_id": int(worker),
                                "seq": int(seq),
                                "seen_elapsed_s": seen_elapsed,
                                "seen_utc": seen_utc,
                                "query_duration_ms": query_duration_ms,
                                "phase": evidence.phase,
                            }
                        )
                        if LAB_METRICS is not None:
                            labels = LAB_METRICS.phase_labels(evidence.phase)
                            LAB_METRICS.commit_visible.labels(**labels).observe(
                                max(0.0, seen_elapsed - commit_elapsed)
                            )
                            LAB_METRICS.visible.labels(**labels).inc()
            except Exception as exc:
                evidence.error("clickhouse_small_observer", exc)
                if LAB_METRICS is not None:
                    LAB_METRICS.observer_errors.labels(
                        **LAB_METRICS.phase_labels(evidence.phase)
                    ).inc()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        await asyncio.to_thread(destination.close)


async def large_observer(
    config: Config,
    run_id: uuid.UUID,
    evidence: Evidence,
    stop: asyncio.Event,
    interval: float,
) -> None:
    destination = await asyncio.to_thread(make_destination, config)
    try:
        while not stop.is_set():
            started = time.perf_counter()
            try:
                row = await asyncio.to_thread(destination.large_state, str(run_id))
                evidence.large.write(
                    {
                        "sampled_at": utc_now(),
                        "elapsed_s": evidence.elapsed(),
                        "run_id": str(run_id),
                        "row_count": int(row[0]),
                        "unique_rows": int(row[1]),
                        "min_row_number": row[2] or "",
                        "max_row_number": row[3] or "",
                        "query_duration_ms": (time.perf_counter() - started) * 1000,
                        "phase": evidence.phase,
                    }
                )
                if LAB_METRICS is not None:
                    labels = LAB_METRICS.labels
                    LAB_METRICS.large_rows.labels(**labels).set(int(row[0]))
                    LAB_METRICS.large_unique.labels(**labels).set(int(row[1]))
                    if evidence.result_dir.name.find("rollback") >= 0:
                        LAB_METRICS.rollback_rows.labels(**labels).set(int(row[0]))
                    LAB_METRICS.observer_query.labels(
                        **LAB_METRICS.phase_labels(evidence.phase)
                    ).observe((time.perf_counter() - started))
            except Exception as exc:
                evidence.error("clickhouse_large_observer", exc)
                if LAB_METRICS is not None:
                    LAB_METRICS.observer_errors.labels(
                        **LAB_METRICS.phase_labels(evidence.phase)
                    ).inc()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        await asyncio.to_thread(destination.close)


async def elephant_transaction(
    config: Config,
    run_id: uuid.UUID,
    outcome: str,
    rows: int,
    payload_size: int,
    hold_seconds: float,
    evidence: Evidence,
) -> None:
    psycopg, _ = await import_pg()
    table = validate_table_name("cdc_lab.cdc_probe_large")
    evidence.set_phase("large_load")
    evidence.event("elephant_begin", rows=rows, outcome=outcome)
    async with await psycopg.AsyncConnection.connect(config.pg_dsn) as conn:
        await conn.execute("SET application_name = 'cdc-elephant-lab'")
        async with conn.cursor() as cur:
            async with cur.copy(
                f"COPY {table} (run_id, outcome, row_number, payload) FROM STDIN"
            ) as copy:
                for number in range(1, rows + 1):
                    await copy.write_row(
                        (
                            run_id,
                            outcome,
                            number,
                            payload_for(run_id, number, payload_size),
                        )
                    )
                    if number % 10_000 == 0:
                        await asyncio.sleep(0)
        evidence.event("elephant_copy_complete", rows=rows)
        evidence.set_phase("open_hold")
        await asyncio.sleep(hold_seconds)
        evidence.set_phase("outcome")
        evidence.event("elephant_outcome_sent", outcome=outcome)
        if outcome == "commit":
            await conn.commit()
        else:
            await conn.rollback()
        evidence.event("elephant_outcome_ack", outcome=outcome)
        evidence.set_phase("post_outcome_drain")


async def guarded(awaitable: Any, safety_stop: asyncio.Event) -> Any:
    """Cancel the workload promptly when a monitor declares the run unsafe."""
    work = asyncio.ensure_future(awaitable)
    alarm = asyncio.create_task(safety_stop.wait())
    done, _ = await asyncio.wait({work, alarm}, return_when=asyncio.FIRST_COMPLETED)
    if alarm in done and safety_stop.is_set() and not work.done():
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        raise RuntimeError(
            "Safety monitor stopped the workload; inspect events.jsonl and errors.jsonl"
        )
    alarm.cancel()
    await asyncio.gather(alarm, return_exceptions=True)
    return await work


async def run_experiment(config: Config, args: argparse.Namespace) -> Path:
    global LAB_METRICS
    if (args.rate >= 100 or args.large_rows >= 100_000) and not args.confirm_full_run:
        raise SystemExit(
            "Refusing a high-volume run without --confirm-full-run. "
            "Run the commit and rollback smoke tests first."
        )
    estimated_rows = (
        int(
            args.rate
            * (
                args.warmup_seconds
                + args.baseline_seconds
                + args.hold_seconds
                + args.recovery_seconds
            )
        )
        + args.large_rows
    )
    estimated_payload = estimated_rows * args.payload_bytes
    print(
        f"Estimated logical workload: {estimated_rows:,} rows, {estimated_payload / (1024**2):.1f} MiB payload. WAL volume will be larger."
    )
    run_id = uuid.uuid4()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result_dir = Path(args.results_dir) / f"{stamp}_{args.outcome}_{run_id}"
    result_dir.mkdir(parents=True)
    evidence = Evidence(result_dir)
    if not args.no_prometheus:
        from metrics import LabMetrics

        LAB_METRICS = LabMetrics(
            args.outcome, config.configuration, args.prometheus_port
        )
    writer_stop = asyncio.Event()
    observer_stop = asyncio.Event()

    preflight_pg = await pg_preflight(config)
    preflight_ch = await asyncio.to_thread(ch_preflight, config)
    metadata = {
        "run_id": str(run_id),
        "outcome": args.outcome,
        "started_utc": utc_now(),
        "repository": "pg-cdc-lab",
        "destination_adapter": "clickhouse",
        "configuration": config.configuration,
        "sync_interval_seconds": config.sync_interval_seconds,
        "pull_batch_size": config.pull_batch_size,
        "clickpipe_metrics": {
            "enabled": config.cloud_metrics_settings() is not None,
            "organization_id": config.cloud_organization_id,
            "clickpipe_id": config.clickpipe_id,
            "poll_seconds": config.clickpipe_metrics_poll_seconds,
        },
        "rate": args.rate,
        "workers": args.workers,
        "warmup_seconds": args.warmup_seconds,
        "baseline_seconds": args.baseline_seconds,
        "large_rows": args.large_rows,
        "payload_bytes": args.payload_bytes,
        "hold_seconds": args.hold_seconds,
        "recovery_seconds": args.recovery_seconds,
        "visibility_poll_seconds": args.visibility_poll_seconds,
        "slot_poll_seconds": args.slot_poll_seconds,
        "observer_drain_seconds": args.observer_drain_seconds,
        "writer_drain_seconds": args.writer_drain_seconds,
        "load_fidelity_threshold": args.load_fidelity_threshold,
        "recovery_threshold": args.recovery_threshold,
        "recovery_window_seconds": args.recovery_window_seconds,
        "prometheus_port": None if args.no_prometheus else args.prometheus_port,
        "environment": {
            "managed_postgres_service_size": os.getenv(
                "PG_CDC_LAB_PG_SERVICE_SIZE", "unknown"
            ),
            "clickhouse_service_size": os.getenv(
                "PG_CDC_LAB_CH_SERVICE_SIZE", "unknown"
            ),
            "managed_postgres_region": os.getenv("PG_CDC_LAB_PG_REGION", "unknown"),
            "clickhouse_region": os.getenv("PG_CDC_LAB_CH_REGION", "unknown"),
            "ha_topology": os.getenv("PG_CDC_LAB_HA_TOPOLOGY", "unknown"),
            "autoscaling_events": [],
        },
        "postgres": preflight_pg,
        "clickhouse": preflight_ch,
        "credentials_redacted": True,
    }
    (result_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (result_dir / "operator_notes.md").write_text(
        "# Operator notes\n\n- ClickPipe sync interval: \n- Pull batch size: \n"
        "- Managed Postgres size/region/HA: \n- ClickHouse size/region: \n"
        "- ClickPipe status/errors: \n- Screenshot filenames: \n- Other observations: \n",
        encoding="utf-8",
    )

    queue: asyncio.Queue[tuple[int, float] | None] = asyncio.Queue(
        maxsize=args.workers * 20
    )
    registry = CommitRegistry(args.workers)
    slot = preflight_pg["slot"]["slot_name"]
    monitor_tasks = [
        asyncio.create_task(
            slot_monitor(
                config,
                slot,
                evidence,
                observer_stop,
                args.slot_poll_seconds,
                args.safe_wal_warning_bytes,
            )
        ),
        asyncio.create_task(
            clickhouse_observer(
                config,
                run_id,
                registry,
                evidence,
                observer_stop,
                args.visibility_poll_seconds,
            )
        ),
        asyncio.create_task(
            large_observer(
                config, run_id, evidence, observer_stop, args.large_poll_seconds
            )
        ),
    ]
    cloud_metrics_settings = config.cloud_metrics_settings()
    if cloud_metrics_settings is not None:
        monitor_tasks.append(
            asyncio.create_task(
                clickpipe_metrics_monitor(
                    cloud_metrics_settings,
                    run_id,
                    evidence,
                    observer_stop,
                    config.clickpipe_metrics_poll_seconds,
                )
            )
        )
    producer_task = asyncio.create_task(
        small_producer(args.rate, queue, evidence, writer_stop)
    )
    workers = [
        asyncio.create_task(
            small_worker(
                worker, config, run_id, args.payload_bytes, queue, registry, evidence
            )
        )
        for worker in range(args.workers)
    ]
    try:
        evidence.event("run_started")
        evidence.set_phase("warmup")
        await guarded(asyncio.sleep(args.warmup_seconds), observer_stop)
        evidence.set_phase("baseline")
        evidence.event("baseline_started")
        await guarded(asyncio.sleep(args.baseline_seconds), observer_stop)
        evidence.event("baseline_complete")
        if not args.load_only:
            await guarded(
                elephant_transaction(
                    config,
                    run_id,
                    args.outcome,
                    args.large_rows,
                    args.payload_bytes,
                    args.hold_seconds,
                    evidence,
                ),
                observer_stop,
            )
            await guarded(asyncio.sleep(args.recovery_seconds), observer_stop)
        evidence.event("run_stop_requested")
    except Exception as exc:
        evidence.error("run_controller", exc)
        evidence.event("run_aborted")
    finally:
        # Stop scheduling first, finish already queued commits, and keep both
        # ClickHouse observers alive long enough to see those final commits.
        writer_stop.set()
        await asyncio.gather(producer_task, return_exceptions=True)
        try:
            await asyncio.wait_for(queue.join(), timeout=args.writer_drain_seconds)
        except asyncio.TimeoutError:
            evidence.event(
                "writer_drain_timeout",
                timeout_seconds=args.writer_drain_seconds,
                queued_jobs=queue.qsize(),
            )
            for worker in workers:
                worker.cancel()
        for _ in workers:
            if not all(worker.done() for worker in workers):
                await queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)

        drain_deadline = evidence.elapsed() + args.observer_drain_seconds
        while evidence.elapsed() < drain_deadline:
            committed, visible = await registry.counts()
            if visible >= committed:
                break
            await asyncio.sleep(min(0.25, args.visibility_poll_seconds))

        await sample_small_integrity(config, run_id, evidence)

        observer_stop.set()
        await asyncio.gather(*monitor_tasks, return_exceptions=True)
        committed, visible = await registry.counts()
        evidence.event("run_finished", committed_small=committed, visible_small=visible)
        evidence.close()
        if LAB_METRICS is not None:
            LAB_METRICS.close()
            LAB_METRICS = None

    from analyze import write_analysis

    write_analysis(result_dir)
    return result_dir


def add_run_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--outcome", choices=["commit", "rollback"], required=True)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--warmup-seconds", type=float, default=0)
    parser.add_argument("--baseline-seconds", type=float, default=600)
    parser.add_argument("--large-rows", type=int, default=1_000_000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--hold-seconds", type=float, default=60)
    parser.add_argument("--recovery-seconds", type=float, default=600)
    parser.add_argument("--visibility-poll-seconds", type=float, default=0.1)
    parser.add_argument("--large-poll-seconds", type=float, default=1.0)
    parser.add_argument("--slot-poll-seconds", type=float, default=1.0)
    parser.add_argument("--observer-drain-seconds", type=float, default=120.0)
    parser.add_argument("--writer-drain-seconds", type=float, default=120.0)
    parser.add_argument("--load-fidelity-threshold", type=float, default=0.95)
    parser.add_argument("--recovery-threshold", type=float, default=1.20)
    parser.add_argument("--recovery-window-seconds", type=float, default=30.0)
    parser.add_argument("--safe-wal-warning-bytes", type=int, default=1_073_741_824)
    parser.add_argument("--prometheus-port", type=int, default=9464)
    parser.add_argument("--no-prometheus", action="store_true")
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Run only the warmup and baseline small-transaction control",
    )
    parser.add_argument("--results-dir", default="results")
    parser.add_argument(
        "--confirm-full-run",
        action="store_true",
        help="Confirm that this isolated test environment may receive a high-volume load",
    )


async def async_main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup-source")
    sub.add_parser("source-preflight")
    sub.add_parser("preflight")
    run_parser = sub.add_parser("run")
    add_run_args(run_parser)
    dry_parser = sub.add_parser("dry-run")
    add_run_args(dry_parser)
    args = parser.parse_args()
    config = Config.from_env(
        require_clickhouse=args.command not in {"setup-source", "source-preflight"}
    )
    # Validate identifiers even in dry-run mode.
    validate_table_name("cdc_lab.cdc_probe_small")
    validate_table_name("cdc_lab.cdc_probe_large")
    if config.ch_small_table:
        ch_table_name(config.ch_small_table)
    if config.ch_large_table:
        ch_table_name(config.ch_large_table)

    if args.command == "setup-source":
        await setup_source(config)
    elif args.command == "source-preflight":
        print(
            json.dumps(
                {"postgres": await source_preflight(config)}, indent=2, default=str
            )
        )
    elif args.command == "preflight":
        pg = await pg_preflight(config)
        ch = await asyncio.to_thread(ch_preflight, config)
        cloud_settings = config.cloud_metrics_settings()
        clickpipe = (
            await asyncio.to_thread(scrape_clickpipe_metrics, cloud_settings)
            if cloud_settings is not None
            else {"status": "not_configured"}
        )
        print(
            json.dumps(
                {"postgres": pg, "clickhouse": ch, "clickpipe": clickpipe},
                indent=2,
                default=str,
            )
        )
    elif args.command == "dry-run":
        safe = vars(args).copy()
        safe.update(
            {
                "pg_dsn": "<redacted>",
                "clickhouse_host": config.ch_host,
                "clickhouse_database": config.ch_database,
                "clickhouse_small_table": config.ch_small_table,
                "clickhouse_large_table": config.ch_large_table,
                "slot": config.slot_name or "<auto-detect exactly one>",
                "clickpipe_metrics_enabled": config.cloud_metrics_settings()
                is not None,
                "clickpipe_id": config.clickpipe_id or "<auto-detect exactly one>",
            }
        )
        print(json.dumps(safe, indent=2))
    else:
        result = await run_experiment(config, args)
        print(f"Evidence written to {result.resolve()}")
        print(f"Open {(result / 'summary.md').resolve()}")


if __name__ == "__main__":
    asyncio.run(async_main())
