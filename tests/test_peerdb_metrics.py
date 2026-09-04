import json
import tempfile
import unittest
from pathlib import Path

from peerdb_metrics import (
    classify_insert,
    metrics_to_events,
    select_raw_table,
    write_artifacts,
)
from plot_peerdb_results import generate, load_metrics


ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "results" / "examples" / "baseline-500k"


class PeerDBMetricsTests(unittest.TestCase):
    def test_baseline_fixture_matches_observed_run(self):
        metrics = json.loads((FIXTURE / "metrics.json").read_text())
        self.assertEqual(metrics["transaction_rows"], 500_000)
        self.assertEqual(metrics["peerdb_batch_ids"], [186])
        self.assertEqual(metrics["raw_record_span_s"], 7.72047)
        self.assertEqual(metrics["commit_to_sync_s"], 9.968626)
        self.assertEqual(metrics["measurement_scope"], "one observed baseline run")

    def test_final_insert_is_classified_before_raw_reference(self):
        query = (
            "INSERT INTO cdc_lab_cdc_probe_large "
            "SELECT * FROM default._peerdb_raw_mirror_123"
        )
        self.assertEqual(
            classify_insert(
                query,
                "default._peerdb_raw_mirror_123",
                "cdc_lab_cdc_probe_large",
            ),
            "final_insert",
        )

    def test_raw_table_discovery_requires_unique_run_match(self):
        self.assertEqual(
            select_raw_table({"default.raw_a": 0, "default.raw_b": 4}), "default.raw_b"
        )
        with self.assertRaisesRegex(ValueError, "multiple"):
            select_raw_table({"default.raw_a": 1, "default.raw_b": 1})
        with self.assertRaisesRegex(ValueError, "No PeerDB"):
            select_raw_table({"default.raw_a": 0})

    def test_missing_query_log_stages_remain_null(self):
        metrics = json.loads((FIXTURE / "metrics.json").read_text())
        events = metrics_to_events(metrics)
        inserts = {
            event["stage"]: event
            for event in events
            if event["stage"].endswith("insert")
        }
        self.assertFalse(inserts["raw_insert"]["available"])
        self.assertIsNone(inserts["final_insert"]["start_ts"])

    def test_artifacts_and_charts_generate_offline(self):
        metrics = load_metrics([FIXTURE])[0]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_artifacts(root / "artifact", metrics, metrics_to_events(metrics))
            self.assertTrue((root / "artifact" / "metrics.csv").exists())
            generate([metrics], root / "charts")
            self.assertTrue(
                (root / "charts" / "latency-vs-transaction-size.svg").exists()
            )
            self.assertTrue((root / "charts" / "peerdb-stage-timeline.svg").exists())
            summary = json.loads((root / "charts" / "sweep-summary.json").read_text())
            self.assertEqual(summary[0]["samples"], 1)
            self.assertEqual(summary[0]["commit_to_sync_s"]["p50"], 9.968626)


if __name__ == "__main__":
    unittest.main()
