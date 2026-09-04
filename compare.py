#!/usr/bin/env python3
"""Compare independent pg-cdc-lab runs; each run is one experimental unit."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from analyze import FINDING_TEXT, write_analysis


def dotted(data: dict[str, Any], path: str) -> Any:
    value: Any = data
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


FIELDS = {
    "baseline_p99_ms": "commit_to_visible_by_phase.baseline.p99_ms",
    "open_hold_p99_ms": "commit_to_visible_by_phase.open_hold.p99_ms",
    "post_outcome_p99_ms": "commit_to_visible_by_phase.post_outcome_drain.p99_ms",
    "peak_retained_wal_bytes": "wal.peak_retained_wal_bytes",
    "overall_recovery_seconds": "recovery.overall_seconds",
    "load_fidelity": "load_fidelity",
    "missing_small": "missing_small_transactions",
    "missing_large": "large_transaction.missing_rows",
    "duplicate_large": "large_transaction.duplicate_rows",
    "rollback_leakage": "large_transaction.rollback_leakage",
}


def summarize_runs(
    result_dirs: list[Path],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    runs = [write_analysis(path) for path in result_dirs]
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        groups[(run["configuration"]["name"], run["outcome"])].append(run)
    aggregate: list[dict[str, Any]] = []
    for (configuration, outcome), members in sorted(groups.items()):
        row: dict[str, Any] = {
            "configuration": configuration,
            "outcome": outcome,
            "runs": len(members),
        }
        for label, path in FIELDS.items():
            values = [
                float(value)
                for member in members
                if (value := dotted(member, path)) is not None
            ]
            row[f"{label}_median"] = statistics.median(values) if values else None
            row[f"{label}_min"] = min(values) if values else None
            row[f"{label}_max"] = max(values) if values else None
        row["excluded_or_invalid_runs"] = sum(
            not member.get("workload_valid", False) for member in members
        )
        aggregate.append(row)
    return runs, aggregate


def write_comparison(result_dirs: list[Path], output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    runs, aggregate = summarize_runs(result_dirs)
    payload = {
        "experimental_unit": "one complete run",
        "runs": runs,
        "aggregate": aggregate,
    }
    (output / "aggregate-results.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
    if aggregate:
        with (output / "aggregate-results.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=list(aggregate[0]))
            writer.writeheader()
            writer.writerows(aggregate)
    eligible = [
        run
        for run in runs
        if run.get("workload_valid") and run.get("correctness", {}).get("passed")
    ]
    finding = Counter(run["finding_class"] for run in eligible).most_common(1)
    finding_class = (
        finding[0][0]
        if finding
        else ("invalid_workload" if runs else "no_material_degradation")
    )
    (output / "finding.md").write_text(
        "# Finding\n\n" + FINDING_TEXT[finding_class] + "\n\n"
        "## Evidence\n\nEvery run is shown in `aggregate-results.csv`; medians and ranges use independent runs only.\n\n"
        "## Likely mechanism\n\nThe selected class is the modal evidence-supported classification among valid, correct runs.\n\n"
        "## What this does not prove\n\nDeployment internals and failure safety require product confirmation and controlled restart/failover experiments.\n\n"
        "## Next experiment\n\nRepeat the most informative endpoints at least five times, changing one workload dimension only.\n\n"
        "## Product implication\n\nPrioritize the mechanism supported by the measured boundary; preserve raw evidence for review.\n",
        encoding="utf-8",
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dirs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, default=Path("comparison"))
    args = parser.parse_args()
    write_comparison(args.result_dirs, args.output)
    print(f"Comparison written to {args.output.resolve()}")


if __name__ == "__main__":
    main()
