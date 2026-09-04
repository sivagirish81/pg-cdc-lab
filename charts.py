"""Deterministic SVG/PNG report graphics derived from raw pg-cdc-lab evidence."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from typing import Any

from analyze import PHASES, classify_phase, phase_boundaries, read_events


def _csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _save(fig: Any, output: Path, name: str) -> None:
    for suffix in ("svg", "png"):
        fig.savefig(
            output / f"{name}.{suffix}",
            dpi=160,
            bbox_inches="tight",
            metadata={"Creator": "pg-cdc-lab"},
        )


def generate_run_charts(
    result_dir: Path, summary: dict[str, Any], output: Path
) -> None:
    matplotlib_cache = Path(tempfile.gettempdir()) / "pg-cdc-lab-matplotlib"
    matplotlib_cache.mkdir(exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(matplotlib_cache))
    os.environ.setdefault("XDG_CACHE_HOME", str(matplotlib_cache))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    output.mkdir(parents=True, exist_ok=True)
    events = read_events(result_dir / "events.jsonl")
    bounds = phase_boundaries(events)
    commits = _csv(result_dir / "small_commits.csv")
    visibility = _csv(result_dir / "small_visibility.csv")
    slots = _csv(result_dir / "slot_samples.csv")
    large = _csv(result_dir / "large_visibility.csv")
    commit_map = {
        (r["worker_id"], r["seq"]): float(r["commit_ack_elapsed_s"]) for r in commits
    }
    latencies = []
    for row in visibility:
        key = (row["worker_id"], row["seq"])
        if key in commit_map:
            latencies.append(
                (
                    float(row["seen_elapsed_s"]),
                    (float(row["seen_elapsed_s"]) - commit_map[key]) * 1000,
                )
            )

    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    axes[0].scatter(
        [x for x, _ in latencies], [y for _, y in latencies], s=3, alpha=0.35
    )
    axes[0].set_ylabel("Commit→visible (ms)")
    axes[1].plot(
        [float(r["elapsed_s"]) for r in slots if r.get("retained_wal_bytes")],
        [
            int(float(r["retained_wal_bytes"])) / 2**20
            for r in slots
            if r.get("retained_wal_bytes")
        ],
    )
    axes[1].set_ylabel("Retained WAL (MiB)")
    axes[2].plot(
        [float(r["elapsed_s"]) for r in large], [int(r["unique_rows"]) for r in large]
    )
    axes[2].set_ylabel("Large unique rows")
    axes[2].set_xlabel("Monotonic elapsed seconds")
    for ax in axes:
        for phase, x in bounds.items():
            ax.axvline(x, linewidth=0.8, alpha=0.35)
    fig.suptitle("pg-cdc-lab transaction boundary")
    _save(fig, output, "transaction-boundary-timeline")
    plt.close(fig)

    phase_stats = summary["commit_to_visible_by_phase"]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(PHASES, [(phase_stats[p]["p99_ms"] or 0) for p in PHASES])
    ax.tick_params(axis="x", rotation=30)
    ax.set_ylabel("p99 (ms)")
    ax.set_title("p99 by commit phase and configuration")
    _save(fig, output, "p99-by-phase-and-configuration")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(
        [float(r["elapsed_s"]) for r in slots if r.get("retained_wal_bytes")],
        [
            int(float(r["retained_wal_bytes"])) / 2**20
            for r in slots
            if r.get("retained_wal_bytes")
        ],
    )
    ax.set(
        xlabel="Monotonic elapsed seconds", ylabel="MiB", title="Retained WAL timeline"
    )
    _save(fig, output, "retained-wal-timeline")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    rec = summary["recovery"]
    names = ["Large visible", "Latency recovery", "WAL recovery", "Overall recovery"]
    values = [
        summary["large_transaction"]["complete_visibility_seconds"],
        rec["latency_seconds"],
        rec["wal_seconds"],
        rec["overall_seconds"],
    ]
    ax.bar(names, [v or 0 for v in values])
    ax.tick_params(axis="x", rotation=20)
    ax.set_ylabel("Seconds")
    _save(fig, output, "catch-up-and-recovery-comparison")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    names = ["Missing small", "Missing large", "Duplicate large", "Rollback leakage"]
    values = [
        summary["missing_small_transactions"],
        summary["large_transaction"]["missing_rows"],
        summary["large_transaction"]["duplicate_rows"],
        summary["large_transaction"]["rollback_leakage"],
    ]
    ax.bar(names, values)
    ax.tick_params(axis="x", rotation=20)
    ax.set_title("Correctness summary")
    _save(fig, output, "correctness-summary")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(
        ["Requested", "Achieved"],
        [summary["requested_tps"] or 0, summary["achieved_tps"] or 0],
    )
    ax.set_ylabel("Transactions/s")
    _save(fig, output, "achieved-versus-requested-tps")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    logical = summary["logical_decoding"]
    ax.bar(
        ["Spill", "Stream"],
        [logical["spill_bytes_delta"] or 0, logical["stream_bytes_delta"] or 0],
    )
    ax.set_ylabel("Byte counter delta")
    _save(fig, output, "spill-versus-stream-deltas")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    samples = [
        v for phase in PHASES for v in [phase_stats[phase]["p99_ms"]] if v is not None
    ]
    ax.boxplot(samples or [0], orientation="vertical")
    ax.set_ylabel("Run phase p99 (ms)")
    ax.set_xticklabels(["This run"])
    _save(fig, output, "run-level-distribution")
    plt.close(fig)
