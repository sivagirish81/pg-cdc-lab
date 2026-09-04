"""Bounded-cardinality Prometheus metrics for pg-cdc-lab."""

from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 90, 120)
ALLOWED_LABELS = frozenset({"scenario", "phase", "configuration"})
PHASE_VALUE = {
    "warmup": 0,
    "baseline": 1,
    "large_load": 2,
    "open_hold": 3,
    "outcome": 4,
    "post_outcome_drain": 5,
    "recovered": 6,
}


class LabMetrics:
    """One per run, using only bounded labels from the measurement contract."""

    def __init__(
        self,
        scenario: str,
        configuration: str,
        port: int = 9464,
        start_server: bool = True,
    ):
        self.registry = CollectorRegistry()
        self.labels = {"scenario": scenario, "configuration": configuration}
        common = ["scenario", "phase", "configuration"]
        stable = ["scenario", "configuration"]
        self.commit_visible = Histogram(
            "pg_cdc_lab_commit_to_visible_seconds",
            "Commit ACK to observer query completion",
            common,
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.source_commit = Histogram(
            "pg_cdc_lab_source_commit_seconds",
            "PostgreSQL transaction execution and commit",
            common,
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.queue_delay = Histogram(
            "pg_cdc_lab_generator_queue_delay_seconds",
            "Open-loop generator queue delay",
            common,
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.observer_query = Histogram(
            "pg_cdc_lab_observer_query_seconds",
            "ClickHouse observer query duration",
            common,
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.committed = Counter(
            "pg_cdc_lab_committed_transactions_total",
            "Committed small transactions",
            common,
            registry=self.registry,
        )
        self.visible = Counter(
            "pg_cdc_lab_visible_transactions_total",
            "Visible small transactions",
            common,
            registry=self.registry,
        )
        self.observer_errors = Counter(
            "pg_cdc_lab_observer_errors_total",
            "Destination observer errors",
            common,
            registry=self.registry,
        )
        self.achieved_tps = Gauge(
            "pg_cdc_lab_achieved_tps",
            "Rolling achieved small transaction rate",
            stable,
            registry=self.registry,
        )
        self.retained_wal = Gauge(
            "pg_cdc_lab_retained_wal_bytes",
            "Current WAL minus restart LSN",
            stable,
            registry=self.registry,
        )
        self.unconfirmed_wal = Gauge(
            "pg_cdc_lab_unconfirmed_wal_bytes",
            "Current WAL minus confirmed flush LSN",
            stable,
            registry=self.registry,
        )
        self.restart_to_confirmed = Gauge(
            "pg_cdc_lab_restart_to_confirmed_bytes",
            "Confirmed flush LSN minus restart LSN",
            stable,
            registry=self.registry,
        )
        self.spill_bytes = Gauge(
            "pg_cdc_lab_spill_bytes",
            "Logical decoding spill bytes",
            stable,
            registry=self.registry,
        )
        self.spill_txns = Gauge(
            "pg_cdc_lab_spill_transactions",
            "Logical decoding spilled transactions",
            stable,
            registry=self.registry,
        )
        self.stream_bytes = Gauge(
            "pg_cdc_lab_stream_bytes",
            "Logical decoding streamed bytes",
            stable,
            registry=self.registry,
        )
        self.stream_txns = Gauge(
            "pg_cdc_lab_stream_transactions",
            "Logical decoding streamed transactions",
            stable,
            registry=self.registry,
        )
        self.large_rows = Gauge(
            "pg_cdc_lab_large_visible_rows",
            "Visible large rows",
            stable,
            registry=self.registry,
        )
        self.large_unique = Gauge(
            "pg_cdc_lab_large_unique_rows",
            "Unique visible large rows",
            stable,
            registry=self.registry,
        )
        self.rollback_rows = Gauge(
            "pg_cdc_lab_rollback_visible_rows",
            "Visible rolled-back rows",
            stable,
            registry=self.registry,
        )
        self.phase = Gauge(
            "pg_cdc_lab_experiment_phase",
            "Numeric experiment phase",
            common,
            registry=self.registry,
        )
        self._server = (
            start_http_server(port, registry=self.registry) if start_server else None
        )

    def phase_labels(self, phase: str) -> dict[str, str]:
        return {**self.labels, "phase": phase}

    def set_phase(self, phase: str) -> None:
        for name, value in PHASE_VALUE.items():
            self.phase.labels(**self.phase_labels(name)).set(
                value if name == phase else -1
            )

    def close(self) -> None:
        server = self._server[0] if isinstance(self._server, tuple) else self._server
        if hasattr(server, "shutdown"):
            server.shutdown()


def metric_label_names(registry: CollectorRegistry) -> set[str]:
    names: set[str] = set()
    for metric in registry.collect():
        for sample in metric.samples:
            names.update(key for key in sample.labels if key != "le")
    return names
