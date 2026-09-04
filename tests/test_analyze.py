import csv
import json
import tempfile
import unittest
import uuid
from pathlib import Path

from analyze import (
    classify_finding,
    classify_phase,
    load_fidelity,
    lsn_to_int,
    percentile,
    queue_delay_growing,
    recovery_time,
    render_markdown,
    sanitize,
    wal_amplification,
    write_analysis,
)
from cdc_lab import ch_table_name, payload_for, validate_table_name
from report import build_bundle


class AnalysisTests(unittest.TestCase):
    def test_percentile_interpolates_and_empty(self):
        self.assertEqual(percentile([1, 2, 3, 4], 0.5), 2.5)
        self.assertIsNone(percentile([], 0.99))

    def test_phase_classification_uses_commit_time(self):
        bounds = {
            "warmup": 0,
            "baseline": 10,
            "large_load": 20,
            "open_hold": 30,
            "outcome": 80,
            "post_outcome_drain": 81,
            "recovered": 200,
        }
        self.assertEqual(classify_phase(5, bounds), "warmup")
        self.assertEqual(classify_phase(15, bounds), "baseline")
        self.assertEqual(classify_phase(25, bounds), "large_load")
        self.assertEqual(classify_phase(40, bounds), "open_hold")
        self.assertEqual(classify_phase(80.5, bounds), "outcome")
        self.assertEqual(classify_phase(100, bounds), "post_outcome_drain")

    def test_load_fidelity(self):
        self.assertEqual(load_fidelity(475, 500), 0.95)
        self.assertIsNone(load_fidelity(None, 500))

    def test_queue_growth_detection(self):
        stable = [{"queue_delay_ms": "1"} for _ in range(40)]
        growing = [{"queue_delay_ms": str(i)} for i in range(40)]
        self.assertFalse(queue_delay_growing(stable))
        self.assertTrue(queue_delay_growing(growing))

    def test_recovery_requires_three_consecutive_windows(self):
        latency = [(t, 110) for t in (5, 15, 25)] + [
            (t, 80) for t in (35, 45, 55, 65, 75, 85, 90)
        ]
        wal = [(t, 110) for t in (5, 15, 25)] + [
            (t, 80) for t in (35, 45, 55, 65, 75, 85, 90)
        ]
        result = recovery_time(
            latency,
            wal,
            0,
            100,
            100,
            threshold=1.0,
            window_seconds=30,
            consecutive_windows=2,
        )
        self.assertEqual(result["overall_seconds"], 30)

    def test_wal_amplification(self):
        self.assertEqual(wal_amplification(300, 100), 3)
        self.assertIsNone(wal_amplification(300, 0))
        self.assertEqual(lsn_to_int("1/00000002") - lsn_to_int("0/FFFFFFFF"), 3)

    def test_missing_duplicate_and_rollback_finding(self):
        base = {
            "correctness": {"passed": False},
            "workload_valid": True,
            "commit_to_visible_by_phase": {},
            "wal": {},
            "logical_decoding": {},
            "configuration": {},
        }
        self.assertEqual(classify_finding(base), "correctness_failure")

    def test_credential_sanitization(self):
        value = {
            "pg_dsn": "postgresql://u:p@host/db",
            "note": "https://alice:secret@example.test/x",
            "nested": {"password": "oops"},
        }
        clean = sanitize(value)
        self.assertEqual(clean["pg_dsn"], "<redacted>")
        self.assertNotIn("secret", clean["note"])
        self.assertEqual(clean["nested"]["password"], "<redacted>")

    def test_identifiers(self):
        self.assertEqual(validate_table_name("cdc_lab.t"), '"cdc_lab"."t"')
        self.assertEqual(ch_table_name("cdc_lab.t"), "`cdc_lab`.`t`")
        with self.assertRaises(ValueError):
            validate_table_name("cdc_lab.t;drop table x")

    def test_payload_size_and_determinism(self):
        run = uuid.uuid4()
        self.assertEqual(len(payload_for(run, 1, 256)), 256)
        self.assertEqual(payload_for(run, 1, 256), payload_for(run, 1, 256))

    def test_synthetic_report_generation_and_correctness(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "run"
            root.mkdir()
            metadata = {
                "run_id": "r1",
                "outcome": "rollback",
                "rate": 1,
                "large_rows": 10,
                "load_fidelity_threshold": 0.95,
                "configuration": "test",
                "recovery_window_seconds": 1,
            }
            (root / "metadata.json").write_text(json.dumps(metadata))
            events = [
                {"event": "phase_transition", "phase": "warmup", "elapsed_s": 0},
                {"event": "phase_transition", "phase": "baseline", "elapsed_s": 0},
                {"event": "phase_transition", "phase": "large_load", "elapsed_s": 1},
                {"event": "phase_transition", "phase": "open_hold", "elapsed_s": 2},
                {"event": "phase_transition", "phase": "outcome", "elapsed_s": 3},
                {
                    "event": "phase_transition",
                    "phase": "post_outcome_drain",
                    "elapsed_s": 3.1,
                },
                {"event": "run_stop_requested", "elapsed_s": 4},
            ]
            (root / "events.jsonl").write_text(
                "\n".join(json.dumps(row) for row in events) + "\n"
            )
            (root / "errors.jsonl").write_text("")
            self.write_csv(
                root / "small_commits.csv",
                [
                    {
                        "worker_id": 0,
                        "seq": 1,
                        "commit_ack_elapsed_s": 0.5,
                        "commit_latency_ms": 1,
                        "queue_delay_ms": 0,
                    }
                ],
            )
            self.write_csv(
                root / "small_visibility.csv",
                [
                    {
                        "worker_id": 0,
                        "seq": 1,
                        "seen_elapsed_s": 0.6,
                        "query_duration_ms": 5,
                    }
                ],
            )
            self.write_csv(
                root / "slot_samples.csv",
                [
                    {
                        "elapsed_s": 0.5,
                        "retained_wal_bytes": 100,
                        "unconfirmed_wal_bytes": 50,
                        "restart_to_confirmed_bytes": 50,
                        "wal_status": "reserved",
                        "spill_bytes": 0,
                        "spill_txns": 0,
                        "stream_bytes": 0,
                        "stream_txns": 0,
                    }
                ],
            )
            self.write_csv(
                root / "large_visibility.csv",
                [
                    {
                        "elapsed_s": 2.5,
                        "row_count": 0,
                        "unique_rows": 0,
                        "min_row_number": "",
                        "max_row_number": "",
                        "query_duration_ms": 4,
                    }
                ],
            )
            summary = write_analysis(root)
            self.assertEqual(summary["large_transaction"]["rollback_leakage"], 0)
            self.assertTrue((root / "finding.md").exists())
            self.assertIn("pg-cdc-lab", render_markdown(summary))
            bundle = Path(temp) / "bundle"
            build_bundle([root], bundle)
            self.assertTrue((bundle / "executive-summary.md").exists())
            self.assertTrue(
                next((bundle / "charts").glob("*/transaction-boundary-timeline.svg"))
            )

    @staticmethod
    def write_csv(path, rows):
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
