#!/usr/bin/env python3
"""Build the shareable pg-cdc-lab evidence bundle."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path

from analyze import fmt_bytes, fmt_ms, sanitize_run_metadata
from charts import generate_run_charts
from compare import write_comparison


def sanitize_evidence_copy(path: Path) -> None:
    metadata = path / "metadata.json"
    if metadata.exists():
        clean = sanitize_run_metadata(json.loads(metadata.read_text(encoding="utf-8")))
        metadata.write_text(json.dumps(clean, indent=2) + "\n", encoding="utf-8")
    metrics = path / "clickpipe_metrics.csv"
    if metrics.exists() and metrics.stat().st_size:
        with metrics.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            fields = reader.fieldnames or []
        for row in rows:
            for private_field in (
                "clickhouse_service_id",
                "clickhouse_service_name",
                "clickpipe_id",
                "clickpipe_name",
            ):
                if row.get(private_field):
                    row[private_field] = "<redacted>"
        with metrics.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)


def build_bundle(result_dirs: list[Path], output: Path) -> None:
    repository = Path(__file__).resolve().parent
    output.mkdir(parents=True, exist_ok=True)
    comparison = write_comparison(result_dirs, output)
    runs = comparison["runs"]
    charts = output / "charts"
    charts.mkdir(exist_ok=True)
    manifests = output / "run-manifests"
    manifests.mkdir(exist_ok=True)
    raw = output / "raw-results"
    raw.mkdir(exist_ok=True)
    for path, summary in zip(result_dirs, runs):
        generate_run_charts(
            path, summary, charts / str(summary.get("run_id") or path.name)
        )
        if (path / "metadata.json").exists():
            metadata = sanitize_run_metadata(
                json.loads((path / "metadata.json").read_text(encoding="utf-8"))
            )
            (manifests / f"{summary.get('run_id')}.json").write_text(
                json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
            )
        target = raw / path.name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(path, target)
        sanitize_evidence_copy(target)
    (output / "grafana").mkdir(exist_ok=True)
    shutil.copy2(
        repository / "observability/grafana/dashboards/pg-cdc-lab.json",
        output / "grafana/dashboard.json",
    )
    (output / "grafana/rendered-panels").mkdir(exist_ok=True)
    shutil.copytree(
        repository / "observability/prometheus",
        output / "prometheus",
        dirs_exist_ok=True,
    )

    representative = runs[0] if runs else {}
    phase = representative.get("commit_to_visible_by_phase", {})
    large = representative.get("large_transaction", {})
    rec = representative.get("recovery", {})
    executive = (
        f"At {representative.get('requested_tps', 'unavailable')} small transactions/sec, a "
        f"{large.get('expected_rows', 'unavailable')}-row transaction changed small-transaction p99 from "
        f"{fmt_ms(phase.get('baseline', {}).get('p99_ms'))} to "
        f"{fmt_ms(phase.get('post_outcome_drain', {}).get('p99_ms'))}, produced "
        f"{fmt_bytes(representative.get('wal', {}).get('peak_retained_wal_bytes'))} of peak retained WAL, "
        f"and required {rec.get('overall_seconds', 'unavailable')} seconds to restore latency and WAL baselines. "
        f"The run produced {large.get('missing_rows', 'unavailable')} missing and "
        f"{large.get('duplicate_rows', 'unavailable')} duplicate large rows; rollback leakage was "
        f"{large.get('rollback_leakage', 'unavailable')}.\n"
    )
    (output / "executive-summary.md").write_text(executive, encoding="utf-8")
    (output / "methodology.md").write_text(
        "# Methodology\n\nCommit-to-visible uses monotonic observer query completion minus PostgreSQL COMMIT acknowledgment. Each complete run is one experimental unit. Raw CSV/JSON is authoritative.\n",
        encoding="utf-8",
    )
    (output / "limitations.md").write_text(
        "# Limitations\n\nResults apply only to recorded service shapes, regions, configurations, workload, and non-failure conditions. Consumer acknowledgment is not treated as destination visibility.\n",
        encoding="utf-8",
    )
    chart_path = (
        f"charts/{representative.get('run_id')}/transaction-boundary-timeline.svg"
        if representative
        else "unavailable"
    )
    (output / "sai-brief.md").write_text(
        "# pg-cdc-lab brief\n\n"
        + (
            representative.get("finding_class", "unavailable").replace("_", " ")
            if representative
            else "No completed comparison"
        )
        + "\n\n"
        f"- Load fidelity: {representative.get('load_fidelity', 'unavailable')}\n"
        f"- Peak retained WAL: {fmt_bytes(representative.get('wal', {}).get('peak_retained_wal_bytes'))}\n"
        f"- Overall recovery: {rec.get('overall_seconds', 'unavailable')} seconds\n\n"
        f"![Transaction boundary]({chart_path})\n\n"
        "Mechanism hypothesis: the timing of latency, WAL, and spill/stream movement identifies whether open-transaction retention or post-commit drain dominates. This does not establish CDC v2 deployment or failure durability. Next, run the identical workload on a confirmed CDC v2 preview or inject a supported restart at the measured staging boundary. I can upstream the correctness and transaction-boundary observability suite needed to make that comparison repeatable.\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dirs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, default=Path("report"))
    args = parser.parse_args()
    build_bundle(args.result_dirs, args.output)
    print(f"Evidence bundle written to {args.output.resolve()}")


if __name__ == "__main__":
    main()
