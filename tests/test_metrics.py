import unittest

from metrics import ALLOWED_LABELS, LabMetrics, metric_label_names


class MetricTests(unittest.TestCase):
    def test_label_cardinality_contract(self):
        metrics = LabMetrics("commit", "test", start_server=False)
        try:
            metrics.commit_visible.labels(**metrics.phase_labels("baseline")).observe(
                0.1
            )
            self.assertLessEqual(metric_label_names(metrics.registry), ALLOWED_LABELS)
        finally:
            metrics.close()


if __name__ == "__main__":
    unittest.main()
