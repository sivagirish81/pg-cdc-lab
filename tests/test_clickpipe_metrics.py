import unittest

from clickpipe_metrics import CloudMetricsSettings, parse_clickpipe_metrics


SAMPLE = """
# HELP ClickPipes_Info State
# TYPE ClickPipes_Info gauge
ClickPipes_Info{clickhouse_service="service-1",clickhouse_service_name="destination",clickpipe_id="pipe-1",clickpipe_name="cdc",clickpipe_source="managed_postgres",clickpipe_status="Running"} 1
# HELP ClickPipes_SourceReplicationLatency_MiB Slot lag
# TYPE ClickPipes_SourceReplicationLatency_MiB gauge
ClickPipes_SourceReplicationLatency_MiB{clickhouse_service="service-1",clickpipe_id="pipe-1"} 32
# HELP ClickPipes_Errors_Total Errors
# TYPE ClickPipes_Errors_Total counter
ClickPipes_Errors_Total{clickhouse_service="service-1",clickpipe_id="pipe-1"} 2
# HELP ClickPipes_CDC_CPUUsage CPU
# TYPE ClickPipes_CDC_CPUUsage gauge
ClickPipes_CDC_CPUUsage{clickhouse_service="service-1"} 0.125
# HELP ClickPipes_CDC_MemoryUsage Memory
# TYPE ClickPipes_CDC_MemoryUsage gauge
ClickPipes_CDC_MemoryUsage{clickhouse_service="service-1"} 4096
"""


class ClickPipeMetricsTests(unittest.TestCase):
    def test_parse_selects_pipe_and_service_metrics(self):
        record = parse_clickpipe_metrics(SAMPLE)
        self.assertEqual(record["clickpipe_id"], "pipe-1")
        self.assertEqual(record["clickpipe_name"], "cdc")
        self.assertEqual(record["clickpipe_status"], "Running")
        self.assertEqual(record["source_replication_latency_mib"], 32)
        self.assertEqual(record["errors_total"], 2)
        self.assertEqual(record["cdc_cpu_usage_cores"], 0.125)
        self.assertEqual(record["cdc_memory_usage_bytes"], 4096)

    def test_requires_id_when_multiple_pipes_exist(self):
        second = SAMPLE + SAMPLE.replace("pipe-1", "pipe-2").replace(
            'clickpipe_name="cdc"', 'clickpipe_name="other"'
        )
        with self.assertRaisesRegex(ValueError, "Multiple ClickPipes"):
            parse_clickpipe_metrics(second)
        self.assertEqual(
            parse_clickpipe_metrics(second, "pipe-2")["clickpipe_id"], "pipe-2"
        )

    def test_endpoint_escapes_organization_and_filters_metrics(self):
        settings = CloudMetricsSettings("org/id", "key", "secret")
        self.assertEqual(
            settings.endpoint,
            "https://api.clickhouse.cloud/v1/organizations/org%2Fid/prometheus"
            "?filtered_metrics=true",
        )


if __name__ == "__main__":
    unittest.main()
