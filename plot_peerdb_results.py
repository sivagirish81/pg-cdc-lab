#!/usr/bin/env python3
"""Generate PeerDB large-transaction charts from canonical metrics artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from analyze import percentile


def parse_ts(value: str) -> datetime:
    value = value.replace("Z", "+00:00")
    # datetime supports microseconds; retain ordering while trimming nanoseconds.
    if "." in value:
        prefix, rest = value.split(".", 1)
        fraction, suffix = rest.split("+", 1) if "+" in rest else (rest, "")
        value = f"{prefix}.{fraction[:6]}" + (f"+{suffix}" if suffix else "")
    return datetime.fromisoformat(value)


def load_metrics(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in paths:
        candidate = path / "metrics.json" if path.is_dir() else path
        rows.append(json.loads(candidate.read_text(encoding="utf-8")))
    return rows


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_size: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        by_size.setdefault(int(row["transaction_rows"]), []).append(row)
    output = []
    for size, samples in sorted(by_size.items()):
        item: dict[str, Any] = {"transaction_rows": size, "samples": len(samples)}
        for field in (
            "commit_to_first_raw_s",
            "raw_record_span_s",
            "final_insert_ms",
            "commit_to_sync_s",
        ):
            values = [
                float(row[field]) for row in samples if row.get(field) is not None
            ]
            item[field] = {
                "count": len(values),
                "p50": statistics.median(values) if values else None,
                "p95": percentile(values, 0.95),
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "standard_deviation": statistics.stdev(values)
                if len(values) > 1
                else (0.0 if values else None),
            }
        output.append(item)
    return output


def generate(rows: list[dict[str, Any]], output: Path) -> None:
    cache = Path(tempfile.gettempdir()) / "pg-cdc-lab-matplotlib"
    cache.mkdir(exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    output.mkdir(parents=True, exist_ok=True)
    summary = aggregate(rows)
    (output / "sweep-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    with (output / "sweep-summary.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        fields = [
            "transaction_rows",
            "samples",
            "metric",
            "count",
            "p50",
            "p95",
            "min",
            "max",
            "standard_deviation",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for item in summary:
            for metric, _ in (
                ("commit_to_first_raw_s", ""),
                ("raw_record_span_s", ""),
                ("final_insert_ms", ""),
                ("commit_to_sync_s", ""),
            ):
                writer.writerow(
                    {
                        "transaction_rows": item["transaction_rows"],
                        "samples": item["samples"],
                        "metric": metric,
                        **item[metric],
                    }
                )
    x = [row["transaction_rows"] for row in summary]
    series = (
        ("commit_to_first_raw_s", "Commit → first raw"),
        ("raw_record_span_s", "Raw CDC timestamp span"),
        ("final_insert_ms", "Final INSERT duration"),
        ("commit_to_sync_s", "Commit → sync timestamp"),
    )
    fig, ax = plt.subplots(figsize=(9, 5))
    for field, label in series:
        values = [
            (
                row[field]["p50"] / 1000
                if field.endswith("_ms") and row[field]["p50"] is not None
                else row[field]["p50"]
            )
            for row in summary
        ]
        ax.plot(x, values, marker="o", label=label)
    ax.set(xlabel="Transaction rows", ylabel="Seconds")
    ax.set_title("Large-transaction latency by size (median per size)")
    ax.legend()
    ax.text(
        0,
        -0.25,
        "Series are overlapping measurements and are not necessarily additive.",
        transform=ax.transAxes,
    )
    fig.savefig(output / "latency-vs-transaction-size.svg", bbox_inches="tight")
    fig.savefig(
        output / "latency-vs-transaction-size.png", dpi=160, bbox_inches="tight"
    )
    plt.close(fig)

    selected = rows[-1]
    commit = parse_ts(selected["commit_ts"])
    point_fields = (
        ("Commit", "commit_ts"),
        ("First raw CDC", "first_raw_ts"),
        ("Last raw CDC", "last_raw_ts"),
        ("Destination sync", "last_sync_ts"),
    )
    fig, ax = plt.subplots(figsize=(10, 4))
    for y, (label, field) in enumerate(point_fields):
        if selected.get(field):
            elapsed = (parse_ts(selected[field]) - commit).total_seconds()
            ax.scatter(elapsed, y, s=60)
            ax.text(elapsed, y + 0.15, label, ha="center")
    for y, (label, start_field, finish_field) in enumerate(
        (
            ("Raw INSERT", "raw_insert_start_ts", "raw_insert_finish_ts"),
            ("Final INSERT", "final_insert_start_ts", "final_insert_finish_ts"),
        ),
        start=len(point_fields),
    ):
        if selected.get(start_field) and selected.get(finish_field):
            start = (parse_ts(selected[start_field]) - commit).total_seconds()
            finish = (parse_ts(selected[finish_field]) - commit).total_seconds()
            ax.barh(y, finish - start, left=start, height=0.35)
            ax.text(start, y + 0.22, label)
    ax.set(
        xlabel="Seconds from PostgreSQL commit",
        yticks=[],
        title=f"Observed wall-clock events: {selected['transaction_rows']:,} rows",
    )
    fig.savefig(output / "peerdb-stage-timeline.svg", bbox_inches="tight")
    fig.savefig(output / "peerdb-stage-timeline.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, default=Path("charts/peerdb"))
    args = parser.parse_args()
    rows = load_metrics(args.inputs)
    generate(rows, args.output)
    print(f"Wrote PeerDB charts to {args.output}")


if __name__ == "__main__":
    main()
