#!/usr/bin/env python3
"""Deterministic offline analysis for pg-cdc-lab result directories."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

PHASES = (
    "warmup",
    "baseline",
    "large_load",
    "open_hold",
    "outcome",
    "post_outcome_drain",
    "recovered",
)
SECRET_KEYS = re.compile(r"(password|passwd|secret|token|dsn|uri|connection)", re.I)
URI_CREDENTIALS = re.compile(
    r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<userinfo>[^/@\s]+)@", re.I
)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def percentile(values: Iterable[float], q: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def statistics(
    values: Iterable[float], suffix: str = "_ms"
) -> dict[str, float | int | None]:
    data = list(values)
    return {
        "count": len(data),
        f"p50{suffix}": percentile(data, 0.50),
        f"p95{suffix}": percentile(data, 0.95),
        f"p99{suffix}": percentile(data, 0.99),
        f"max{suffix}": max(data) if data else None,
    }


def phase_boundaries(events: list[dict[str, Any]]) -> dict[str, float]:
    boundaries: dict[str, float] = {}
    for event in events:
        elapsed = float(event.get("elapsed_s", 0))
        if event.get("event") == "phase_transition" and event.get("phase") in PHASES:
            boundaries[str(event["phase"])] = elapsed
        mapping = {
            "run_started": "warmup",
            "baseline_started": "baseline",
            "elephant_begin": "large_load",
            "elephant_copy_complete": "open_hold",
            "elephant_outcome_sent": "outcome",
            "elephant_outcome_ack": "post_outcome_drain",
            "recovered": "recovered",
        }
        if event.get("event") in mapping:
            boundaries.setdefault(mapping[str(event["event"])], elapsed)
    return boundaries


def classify_phase(elapsed: float, boundaries: dict[str, float]) -> str:
    selected = "warmup"
    for phase in PHASES:
        if elapsed >= boundaries.get(phase, math.inf):
            selected = phase
    return selected


def load_fidelity(
    achieved_tps: float | None, requested_tps: float | None
) -> float | None:
    if achieved_tps is None or not requested_tps:
        return None
    return achieved_tps / requested_tps


def queue_delay_growing(
    rows: list[dict[str, str]], minimum_growth_ms: float = 10.0
) -> bool:
    values = [float(row["queue_delay_ms"]) for row in rows if row.get("queue_delay_ms")]
    if len(values) < 20:
        return False
    width = max(5, len(values) // 4)
    first = percentile(values[:width], 0.50) or 0
    last = percentile(values[-width:], 0.50) or 0
    return last - first > minimum_growth_ms and last > max(
        first * 1.5, minimum_growth_ms
    )


def wal_amplification(peak: int | None, baseline_p95: float | None) -> float | None:
    if peak is None or not baseline_p95 or baseline_p95 <= 0:
        return None
    return peak / baseline_p95


def lsn_to_int(value: str | None) -> int | None:
    if not value or "/" not in value:
        return None
    high, low = value.split("/", 1)
    try:
        return (int(high, 16) << 32) + int(low, 16)
    except ValueError:
        return None


def sanitize(value: Any) -> Any:
    """Recursively redact credential-bearing fields and URI userinfo."""
    if isinstance(value, dict):
        return {
            key: "<redacted>" if SECRET_KEYS.search(str(key)) else sanitize(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        return URI_CREDENTIALS.sub(r"\g<scheme><redacted>@", value)
    return value


def _float(row: dict[str, str], key: str) -> float | None:
    try:
        return float(row[key]) if row.get(key) not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _int(row: dict[str, str], key: str) -> int | None:
    value = _float(row, key)
    return int(value) if value is not None else None


def recovery_time(
    latency_points: list[tuple[float, float]],
    wal_points: list[tuple[float, float]],
    outcome_elapsed: float | None,
    baseline_latency_p99: float | None,
    baseline_wal_p95: float | None,
    threshold: float = 1.20,
    window_seconds: float = 30.0,
    consecutive_windows: int = 3,
) -> dict[str, float | None]:
    """Return first sustained latency/WAL/combined recovery after outcome ACK."""
    result = {"latency_seconds": None, "wal_seconds": None, "overall_seconds": None}
    if outcome_elapsed is None:
        return result

    end = max(
        [outcome_elapsed]
        + [point[0] for point in latency_points]
        + [point[0] for point in wal_points]
    )
    window_start = outcome_elapsed
    streak_latency = streak_wal = streak_both = 0
    while window_start + window_seconds <= end + 1e-9:
        window_end = window_start + window_seconds
        latencies = [v for t, v in latency_points if window_start <= t < window_end]
        wal = [v for t, v in wal_points if window_start <= t < window_end]
        latency_value = percentile(latencies, 0.99)
        wal_value = percentile(wal, 0.95)
        latency_ok = (
            latency_value is not None
            and baseline_latency_p99 is not None
            and latency_value <= threshold * baseline_latency_p99
        )
        wal_ok = (
            wal_value is not None
            and baseline_wal_p95 is not None
            and wal_value <= threshold * baseline_wal_p95
        )
        streak_latency = streak_latency + 1 if latency_ok else 0
        streak_wal = streak_wal + 1 if wal_ok else 0
        streak_both = streak_both + 1 if latency_ok and wal_ok else 0
        recovery_at = (
            window_end - consecutive_windows * window_seconds - outcome_elapsed
        )
        recovery_at = max(0.0, recovery_at)
        if streak_latency >= consecutive_windows and result["latency_seconds"] is None:
            result["latency_seconds"] = recovery_at
        if streak_wal >= consecutive_windows and result["wal_seconds"] is None:
            result["wal_seconds"] = recovery_at
        if streak_both >= consecutive_windows and result["overall_seconds"] is None:
            result["overall_seconds"] = recovery_at
        window_start = window_end
    return result


def classify_finding(summary: dict[str, Any]) -> str:
    correctness = summary.get("correctness", {})
    if not correctness.get("passed", False):
        return "correctness_failure"
    if not summary.get("workload_valid", False):
        return "invalid_workload"
    phases = summary.get("commit_to_visible_by_phase", {})
    baseline = phases.get("baseline", {}).get("p99_ms")
    hold = phases.get("open_hold", {}).get("p99_ms")
    drain = phases.get("post_outcome_drain", {}).get("p99_ms")
    wal = summary.get("wal", {})
    stream_delta = (
        summary.get("logical_decoding", {}).get("stream_bytes_pre_outcome_delta", 0)
        or 0
    )
    if stream_delta > 0:
        return "in_progress_streaming"
    if baseline and hold and hold > baseline * 1.5:
        return "precommit_head_of_line"
    if baseline and drain and drain > baseline * 1.5:
        return "commit_time_burst"
    if (wal.get("amplification") or 0) >= 2:
        return "wal_retention_pressure"
    if (
        baseline
        and baseline
        >= 0.5
        * float(summary.get("configuration", {}).get("sync_interval_seconds") or 0)
        * 1000
    ):
        return "batch_interval_dominates"
    return "no_material_degradation"


def analyze(result_dir: Path) -> dict[str, Any]:
    events = read_events(result_dir / "events.jsonl")
    boundaries = phase_boundaries(events)
    commits = read_csv(result_dir / "small_commits.csv")
    visibility = read_csv(result_dir / "small_visibility.csv")
    slots = read_csv(result_dir / "slot_samples.csv")
    large = read_csv(result_dir / "large_visibility.csv")
    small_integrity = read_csv(result_dir / "small_integrity.csv")
    clickpipe = read_csv(result_dir / "clickpipe_metrics.csv")
    errors = read_events(result_dir / "errors.jsonl")
    metadata_path = result_dir / "metadata.json"
    metadata = (
        sanitize(json.loads(metadata_path.read_text()))
        if metadata_path.exists()
        else {}
    )

    commit_map = {(int(row["worker_id"]), int(row["seq"])): row for row in commits}
    commit_times = sorted(float(row["commit_ack_elapsed_s"]) for row in commits)
    measured_duration = None
    if (
        boundaries.get("baseline") is not None
        and boundaries.get("post_outcome_drain") is not None
    ):
        stop = next(
            (
                float(e["elapsed_s"])
                for e in reversed(events)
                if e.get("event") == "run_stop_requested"
            ),
            None,
        )
        measured_duration = (stop - boundaries["baseline"]) if stop else None
    measured_commits = [t for t in commit_times if t >= boundaries.get("baseline", 0)]
    if measured_duration and measured_duration > 0:
        achieved_tps = len(measured_commits) / measured_duration
    elif len(commit_times) > 1 and commit_times[-1] > commit_times[0]:
        achieved_tps = (len(commit_times) - 1) / (commit_times[-1] - commit_times[0])
    else:
        achieved_tps = None

    by_phase: dict[str, list[float]] = defaultdict(list)
    visible_keys: set[tuple[int, int]] = set()
    latency_points: list[tuple[float, float]] = []
    observer_durations: list[float] = []
    for row in visibility:
        key = (int(row["worker_id"]), int(row["seq"]))
        commit = commit_map.get(key)
        if commit is None or key in visible_keys:
            continue
        visible_keys.add(key)
        commit_elapsed = float(commit["commit_ack_elapsed_s"])
        latency_ms = max(0.0, (float(row["seen_elapsed_s"]) - commit_elapsed) * 1000)
        by_phase[classify_phase(commit_elapsed, boundaries)].append(latency_ms)
        latency_points.append((commit_elapsed, latency_ms))
        if row.get("query_duration_ms"):
            observer_durations.append(float(row["query_duration_ms"]))

    for row in large:
        if row.get("query_duration_ms"):
            observer_durations.append(float(row["query_duration_ms"]))

    slot_by_phase: dict[str, list[int]] = defaultdict(list)
    wal_points: list[tuple[float, float]] = []
    for row in slots:
        value = _int(row, "retained_wal_bytes")
        elapsed = _float(row, "elapsed_s")
        if value is not None and elapsed is not None:
            slot_by_phase[classify_phase(elapsed, boundaries)].append(value)
            wal_points.append((elapsed, float(value)))

    baseline_latency = percentile(by_phase.get("baseline", []), 0.99)
    baseline_wal = percentile(slot_by_phase.get("baseline", []), 0.95)
    peak_wal = max(
        (value for values in slot_by_phase.values() for value in values), default=None
    )
    outcome_elapsed = boundaries.get("post_outcome_drain")
    threshold = float(metadata.get("recovery_threshold", 1.20))
    window = float(metadata.get("recovery_window_seconds", 30.0))
    recovery = recovery_time(
        latency_points,
        wal_points,
        outcome_elapsed,
        baseline_latency,
        baseline_wal,
        threshold=threshold,
        window_seconds=window,
    )
    recovered_elapsed = (
        outcome_elapsed + recovery["overall_seconds"]
        if outcome_elapsed is not None and recovery["overall_seconds"] is not None
        else None
    )
    recovered_utc = None
    if recovered_elapsed is not None:
        outcome_event = next(
            (event for event in events if event.get("event") == "elephant_outcome_ack"),
            None,
        )
        if outcome_event and outcome_event.get("utc"):
            recovered_utc = (
                datetime.fromisoformat(str(outcome_event["utc"]))
                + timedelta(seconds=float(recovery["overall_seconds"]))
            ).isoformat(timespec="milliseconds")
        boundaries["recovered"] = recovered_elapsed
        by_phase = defaultdict(list)
        for row in visibility:
            key = (int(row["worker_id"]), int(row["seq"]))
            commit = commit_map.get(key)
            if commit is None:
                continue
            commit_elapsed = float(commit["commit_ack_elapsed_s"])
            latency_ms = max(
                0.0, (float(row["seen_elapsed_s"]) - commit_elapsed) * 1000
            )
            by_phase[classify_phase(commit_elapsed, boundaries)].append(latency_ms)

    max_large_count = max((_int(row, "row_count") or 0 for row in large), default=0)
    max_large_unique = max((_int(row, "unique_rows") or 0 for row in large), default=0)
    last_large = large[-1] if large else {}
    last_small_integrity = small_integrity[-1] if small_integrity else {}
    expected_large = int(metadata.get("large_rows", 0) or 0)
    outcome = metadata.get("outcome")
    duplicate_large = max(0, max_large_count - max_large_unique)
    missing_large = (
        max(0, expected_large - max_large_unique) if outcome == "commit" else 0
    )
    rollback_leakage = max_large_count if outcome == "rollback" else 0
    destination_small_count = _int(last_small_integrity, "row_count")
    destination_small_unique = _int(last_small_integrity, "unique_rows")
    duplicate_small_rows = (
        max(0, destination_small_count - destination_small_unique)
        if destination_small_count is not None and destination_small_unique is not None
        else None
    )
    min_number = _int(last_large, "min_row_number")
    max_number = _int(last_large, "max_row_number")
    large_correct = (
        (
            max_large_count == expected_large
            and max_large_unique == expected_large
            and min_number == 1
            and max_number == expected_large
        )
        if outcome == "commit"
        else rollback_leakage == 0
    )

    wal_statuses = {row.get("wal_status") for row in slots if row.get("wal_status")}
    slot_lost = "lost" in wal_statuses
    requested = float(metadata.get("rate", 0) or 0)
    fidelity = load_fidelity(achieved_tps, requested)
    fidelity_threshold = float(metadata.get("load_fidelity_threshold", 0.95))
    queue_growth = queue_delay_growing(commits)
    workload_valid = (
        fidelity is not None and fidelity >= fidelity_threshold and not queue_growth
    )

    def counter_delta(field: str) -> int | None:
        values = [_int(row, field) for row in slots]
        valid = [value for value in values if value is not None]
        return valid[-1] - valid[0] if valid else None

    def counter_delta_before_outcome(field: str) -> int | None:
        values = [
            _int(row, field)
            for row in slots
            if outcome_elapsed is None
            or (_float(row, "elapsed_s") or 0) <= outcome_elapsed
        ]
        valid = [value for value in values if value is not None]
        return valid[-1] - valid[0] if valid else None

    restart_valid = [
        (row.get("restart_lsn"), value)
        for row in slots
        if (value := lsn_to_int(row.get("restart_lsn"))) is not None
    ]
    confirmed_valid = [
        (row.get("confirmed_flush_lsn"), value)
        for row in slots
        if (value := lsn_to_int(row.get("confirmed_flush_lsn"))) is not None
    ]
    clickpipe_durations = [
        float(row["batch_duration_seconds"])
        for row in clickpipe
        if row.get("batch_duration_seconds")
    ]

    def clickpipe_values(field: str) -> list[float]:
        return [value for row in clickpipe if (value := _float(row, field)) is not None]

    def clickpipe_counter_delta(field: str) -> float | None:
        values = clickpipe_values(field)
        if not values:
            return None
        delta = values[-1] - values[0]
        return delta if delta >= 0 else None

    clickpipe_statuses = sorted(
        {row["clickpipe_status"] for row in clickpipe if row.get("clickpipe_status")}
    )

    large_complete_at = (
        next(
            (
                _float(row, "elapsed_s")
                for row in large
                if _int(row, "unique_rows") == expected_large
            ),
            None,
        )
        if outcome == "commit"
        else None
    )
    complete_visibility_seconds = (
        large_complete_at - outcome_elapsed
        if large_complete_at is not None and outcome_elapsed is not None
        else None
    )
    throughput = (
        expected_large / complete_visibility_seconds
        if complete_visibility_seconds and complete_visibility_seconds > 0
        else None
    )
    phase_stats = {phase: statistics(by_phase.get(phase, [])) for phase in PHASES}

    summary: dict[str, Any] = {
        "schema_version": 3,
        "run_id": metadata.get("run_id"),
        "outcome": outcome,
        "configuration": {
            "name": metadata.get("configuration", "unknown"),
            "sync_interval_seconds": metadata.get("sync_interval_seconds"),
            "pull_batch_size": metadata.get("pull_batch_size"),
        },
        "requested_tps": requested,
        "achieved_tps": achieved_tps,
        "load_fidelity": fidelity,
        "load_fidelity_threshold": fidelity_threshold,
        "queue_delay_growing": queue_growth,
        "workload_valid": workload_valid,
        "source_commit_latency": statistics(
            float(row["commit_latency_ms"]) for row in commits
        ),
        "queue_delay": statistics(float(row["queue_delay_ms"]) for row in commits),
        "committed_small_transactions": len(commits),
        "observed_small_transactions": len(visible_keys),
        "missing_small_transactions": len(commits) - len(visible_keys),
        "duplicate_small_observations": max(0, len(visibility) - len(visible_keys)),
        "destination_small_row_count": destination_small_count,
        "destination_small_unique_rows": destination_small_unique,
        "duplicate_small_rows": duplicate_small_rows,
        "commit_to_visible_all": statistics(
            v for values in by_phase.values() for v in values
        ),
        "commit_to_visible_by_phase": phase_stats,
        "p99_amplification_by_phase": {
            phase: (
                phase_stats[phase]["p99_ms"] / baseline_latency
                if baseline_latency and phase_stats[phase]["p99_ms"] is not None
                else None
            )
            for phase in PHASES
        },
        "observer_query_latency": statistics(observer_durations),
        "clickpipe_batch_duration": statistics(clickpipe_durations, suffix="_seconds"),
        "clickpipe": {
            "samples": len(clickpipe),
            "clickpipe_id": next(
                (
                    row.get("clickpipe_id")
                    for row in clickpipe
                    if row.get("clickpipe_id")
                ),
                None,
            ),
            "clickpipe_name": next(
                (
                    row.get("clickpipe_name")
                    for row in clickpipe
                    if row.get("clickpipe_name")
                ),
                None,
            ),
            "statuses": clickpipe_statuses,
            "source_replication_latency": statistics(
                clickpipe_values("source_replication_latency_mib"), suffix="_mib"
            ),
            "errors_delta": clickpipe_counter_delta("errors_total"),
            "fetched_events_delta": clickpipe_counter_delta("fetched_events_total"),
            "sent_events_delta": clickpipe_counter_delta("sent_events_total"),
            "fetched_bytes_delta": clickpipe_counter_delta("fetched_bytes_total"),
            "fetched_compressed_bytes_delta": clickpipe_counter_delta(
                "fetched_bytes_compressed_total"
            ),
            "sent_bytes_delta": clickpipe_counter_delta("sent_bytes_total"),
            "sent_compressed_bytes_delta": clickpipe_counter_delta(
                "sent_bytes_compressed_total"
            ),
            "cdc_cpu_usage": statistics(
                clickpipe_values("cdc_cpu_usage_cores"), suffix="_cores"
            ),
            "cdc_memory_usage": statistics(
                clickpipe_values("cdc_memory_usage_bytes"), suffix="_bytes"
            ),
            "cdc_network_receive_60s": statistics(
                clickpipe_values("cdc_network_receive_bytes_60s"), suffix="_bytes"
            ),
        },
        "wal": {
            "baseline_retained_wal_p95_bytes": baseline_wal,
            "peak_retained_wal_bytes": peak_wal,
            "peak_unconfirmed_wal_bytes": max(
                (_int(r, "unconfirmed_wal_bytes") or 0 for r in slots), default=None
            ),
            "peak_restart_to_confirmed_bytes": max(
                (_int(r, "restart_to_confirmed_bytes") or 0 for r in slots),
                default=None,
            ),
            "amplification": wal_amplification(peak_wal, baseline_wal),
            "statuses": sorted(wal_statuses),
            "restart_lsn_first": restart_valid[0][0] if restart_valid else None,
            "restart_lsn_last": restart_valid[-1][0] if restart_valid else None,
            "restart_lsn_movement_bytes": (
                restart_valid[-1][1] - restart_valid[0][1] if restart_valid else None
            ),
            "confirmed_flush_lsn_first": confirmed_valid[0][0]
            if confirmed_valid
            else None,
            "confirmed_flush_lsn_last": confirmed_valid[-1][0]
            if confirmed_valid
            else None,
            "confirmed_flush_lsn_movement_bytes": (
                confirmed_valid[-1][1] - confirmed_valid[0][1]
                if confirmed_valid
                else None
            ),
        },
        "peak_retained_wal_bytes_by_phase": {
            phase: max(slot_by_phase[phase])
            for phase in PHASES
            if slot_by_phase.get(phase)
        },
        "logical_decoding": {
            "spill_bytes_delta": counter_delta("spill_bytes"),
            "spill_txns_delta": counter_delta("spill_txns"),
            "stream_bytes_delta": counter_delta("stream_bytes"),
            "stream_txns_delta": counter_delta("stream_txns"),
            "spill_bytes_pre_outcome_delta": counter_delta_before_outcome(
                "spill_bytes"
            ),
            "stream_bytes_pre_outcome_delta": counter_delta_before_outcome(
                "stream_bytes"
            ),
        },
        "large_transaction": {
            "expected_rows": expected_large,
            "max_visible_rows": max_large_count,
            "max_unique_rows": max_large_unique,
            "missing_rows": missing_large,
            "duplicate_rows": duplicate_large,
            "rollback_leakage": rollback_leakage,
            "complete_visibility_seconds": complete_visibility_seconds,
            "visibility_throughput_rows_per_second": throughput,
            "final_observation": last_large,
        },
        "recovery": {
            **recovery,
            "threshold_multiplier": threshold,
            "window_seconds": window,
            "required_consecutive_windows": 3,
            "recovered_elapsed_s": recovered_elapsed,
            "recovered_utc": recovered_utc,
        },
        "correctness": {
            "passed": not errors
            and not slot_lost
            and len(commits) == len(visible_keys)
            and large_correct
            and duplicate_small_rows in {0, None}
            and destination_small_unique in {len(commits), None},
            "large_atomicity_passed": large_correct,
            "slot_never_lost": not slot_lost,
            "errors": len(errors),
        },
        "errors": errors,
        "boundaries_elapsed_s": boundaries,
        "metadata": metadata,
        "measurement_note": "Commit-to-visible is an upper bound: destination query completion minus PostgreSQL COMMIT acknowledgment; it includes polling delay and query execution.",
    }
    summary["finding_class"] = classify_finding(summary)
    return sanitize(summary)


FINDING_TEXT = {
    "commit_time_burst": "The tested CDC path isolated small transactions while the large transaction was open, but post-commit delivery created a tail-latency burst.",
    "precommit_head_of_line": "An open large transaction introduced measurable head-of-line effects for independent committed transactions.",
    "wal_retention_pressure": "The tested path protected foreground visibility latency while shifting large-transaction cost into logical-slot WAL retention.",
    "in_progress_streaming": "The tested path streamed or staged in-progress transaction data while preserving destination atomicity in the non-failure case.",
    "batch_interval_dominates": "Configured batch scheduling dominated customer-visible latency in this run.",
    "correctness_failure": "At least one transaction correctness invariant failed; use the smallest reproducible workload before drawing performance conclusions.",
    "invalid_workload": "The run did not sustain the requested source workload and is invalid for CDC performance comparison.",
    "no_material_degradation": "Within the tested workload envelope, no material transaction-boundary degradation was established.",
}


def render_finding(summary: dict[str, Any]) -> str:
    finding = summary["finding_class"]
    phase = summary.get("commit_to_visible_by_phase", {})
    wal = summary.get("wal", {})
    return "\n".join(
        [
            "# Finding",
            "",
            FINDING_TEXT[finding],
            "",
            "## Evidence",
            "",
            f"- Baseline small p99: {fmt_ms(phase.get('baseline', {}).get('p99_ms'))}",
            f"- Open-hold small p99: {fmt_ms(phase.get('open_hold', {}).get('p99_ms'))}",
            f"- Post-outcome small p99: {fmt_ms(phase.get('post_outcome_drain', {}).get('p99_ms'))}",
            f"- Peak retained WAL: {fmt_bytes(wal.get('peak_retained_wal_bytes'))}",
            f"- Load fidelity: {fmt_ratio(summary.get('load_fidelity'))}",
            "",
            "## Likely mechanism",
            "",
            "This classification follows the measured latency, WAL, spill/stream, workload-validity, and correctness signals. It is a mechanism hypothesis, not deployment proof.",
            "",
            "## What this does not prove",
            "",
            "A single run does not establish population-level behavior, production topology, CDC v2 deployment, or failure safety.",
            "",
            "## Next experiment",
            "",
            "Repeat independent runs, hold all service variables constant, and vary only the dominant transaction dimension identified here.",
            "",
            "## Product implication",
            "",
            "Use the quantified boundary to prioritize staging, drain control, WAL-risk telemetry, or lower scheduling latency as supported by the evidence.",
            "",
        ]
    )


def fmt_ms(value: Any) -> str:
    return "unavailable" if value is None else f"{float(value):.1f} ms"


def fmt_bytes(value: Any) -> str:
    return "unavailable" if value is None else f"{float(value) / (1024**2):.1f} MiB"


def fmt_ratio(value: Any) -> str:
    return "unavailable" if value is None else f"{float(value):.3f}"


def fmt_mib(value: Any) -> str:
    return "unavailable" if value is None else f"{float(value):.1f} MiB"


def fmt_cores(value: Any) -> str:
    return "unavailable" if value is None else f"{float(value):.3f} cores"


def render_markdown(summary: dict[str, Any]) -> str:
    clickpipe = summary.get("clickpipe", {})
    lines = [
        "# pg-cdc-lab run summary",
        "",
        f"- Run: `{summary.get('run_id')}`",
        f"- Configuration: `{summary.get('configuration', {}).get('name')}`",
        f"- Outcome: `{summary.get('outcome')}`",
        f"- Load fidelity: `{fmt_ratio(summary.get('load_fidelity'))}` (valid: `{summary.get('workload_valid')}`)",
        f"- Correctness passed: `{summary.get('correctness', {}).get('passed')}`",
        "",
        "## Commit-to-visible latency",
        "",
        "| Commit phase | Samples | p50 | p95 | p99 | max |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for phase in PHASES:
        stats = summary["commit_to_visible_by_phase"][phase]
        lines.append(
            f"| {phase} | {stats['count']} | {fmt_ms(stats['p50_ms'])} | {fmt_ms(stats['p95_ms'])} | {fmt_ms(stats['p99_ms'])} | {fmt_ms(stats['max_ms'])} |"
        )
    lines.extend(
        [
            "",
            "## WAL and recovery",
            "",
            f"- Peak retained WAL: `{fmt_bytes(summary['wal']['peak_retained_wal_bytes'])}`",
            f"- Peak unconfirmed WAL: `{fmt_bytes(summary['wal']['peak_unconfirmed_wal_bytes'])}`",
            f"- Latency recovery: `{summary['recovery']['latency_seconds']}` seconds",
            f"- WAL recovery: `{summary['recovery']['wal_seconds']}` seconds",
            f"- Overall recovery: `{summary['recovery']['overall_seconds']}` seconds",
            "",
            "## ClickPipe",
            "",
            f"- Samples: `{clickpipe.get('samples', 0)}`",
            f"- Statuses: `{', '.join(clickpipe.get('statuses', [])) or 'unavailable'}`",
            f"- Source slot lag p95: `{fmt_mib(clickpipe.get('source_replication_latency', {}).get('p95_mib'))}`",
            f"- Errors during run: `{clickpipe.get('errors_delta')}`",
            f"- CDC CPU p95: `{fmt_cores(clickpipe.get('cdc_cpu_usage', {}).get('p95_cores'))}`",
            f"- CDC memory p95: `{fmt_bytes(clickpipe.get('cdc_memory_usage', {}).get('p95_bytes'))}`",
            "",
            "## Correctness",
            "",
            f"- Missing small rows: `{summary['missing_small_transactions']}`",
            f"- Duplicate small rows: `{summary['duplicate_small_rows']}`",
            f"- Missing large rows: `{summary['large_transaction']['missing_rows']}`",
            f"- Duplicate large rows: `{summary['large_transaction']['duplicate_rows']}`",
            f"- Rollback leakage: `{summary['large_transaction']['rollback_leakage']}`",
            f"- Logical slot never lost: `{summary['correctness']['slot_never_lost']}`",
            "",
            f"> {summary['measurement_note']}",
            "",
        ]
    )
    return "\n".join(lines)


def write_analysis(result_dir: Path) -> dict[str, Any]:
    summary = analyze(result_dir)
    (result_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    (result_dir / "summary.md").write_text(render_markdown(summary), encoding="utf-8")
    (result_dir / "finding.md").write_text(render_finding(summary), encoding="utf-8")
    derived = []
    if summary.get("recovery", {}).get("recovered_elapsed_s") is not None:
        derived.append(
            {
                "event": "recovered",
                "phase": "recovered",
                "elapsed_s": summary["recovery"]["recovered_elapsed_s"],
                "utc": summary["recovery"]["recovered_utc"],
                "derived_offline": True,
                "definition": "latency and WAL thresholds held for three consecutive windows",
            }
        )
    (result_dir / "analysis_events.jsonl").write_text(
        "".join(json.dumps(event, sort_keys=True) + "\n" for event in derived),
        encoding="utf-8",
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    print(render_markdown(write_analysis(args.result_dir)))


if __name__ == "__main__":
    main()
