"""ClickHouse Cloud Prometheus collection for ClickPipe run evidence."""

from __future__ import annotations

import base64
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from prometheus_client.parser import text_string_to_metric_families


PIPE_METRICS = {
    "ClickPipes_SourceReplicationLatency_MiB": "source_replication_latency_mib",
    "ClickPipes_Errors_Total": "errors_total",
    "ClickPipes_FetchedEvents_Total": "fetched_events_total",
    "ClickPipes_SentEvents_Total": "sent_events_total",
    "ClickPipes_FetchedBytes_Total": "fetched_bytes_total",
    "ClickPipes_FetchedBytesCompressed_Total": "fetched_bytes_compressed_total",
    "ClickPipes_SentBytes_Total": "sent_bytes_total",
    "ClickPipes_SentBytesCompressed_Total": "sent_bytes_compressed_total",
    "ClickPipes_FetchedBytesInitialLoad_Total": "fetched_bytes_initial_load_total",
    "ClickPipes_FetchedBytesResync_Total": "fetched_bytes_resync_total",
    "ClickPipes_Replica_CPULimit": "replica_cpu_limit_cores",
    "ClickPipes_Replica_MemoryLimit": "replica_memory_limit_bytes",
}

SERVICE_METRICS = {
    "ClickPipes_CDC_CPUUsage": "cdc_cpu_usage_cores",
    "ClickPipes_CDC_CPULimit": "cdc_cpu_limit_cores",
    "ClickPipes_CDC_MemoryUsage": "cdc_memory_usage_bytes",
    "ClickPipes_CDC_MemoryLimit": "cdc_memory_limit_bytes",
    "ClickPipes_CDC_NetworkReceiveBytes": "cdc_network_receive_bytes_60s",
}

CSV_FIELDS = [
    "run_id",
    "sampled_at",
    "elapsed_s",
    "phase",
    "clickhouse_service_id",
    "clickhouse_service_name",
    "clickpipe_id",
    "clickpipe_name",
    "clickpipe_source",
    "clickpipe_status",
    *PIPE_METRICS.values(),
    *SERVICE_METRICS.values(),
    # Retained for optional manual batch transcription and old-run compatibility.
    "batch_start_utc",
    "batch_end_utc",
    "batch_duration_seconds",
    "rows",
    "notes",
]


@dataclass(frozen=True)
class CloudMetricsSettings:
    organization_id: str
    api_key_id: str
    api_key_secret: str
    clickpipe_id: str | None = None
    api_base_url: str = "https://api.clickhouse.cloud"
    timeout_seconds: float = 10.0

    @property
    def endpoint(self) -> str:
        organization = urllib.parse.quote(self.organization_id, safe="")
        return (
            f"{self.api_base_url.rstrip('/')}/v1/organizations/{organization}/"
            "prometheus?filtered_metrics=true"
        )


def fetch_prometheus_text(settings: CloudMetricsSettings) -> str:
    token = base64.b64encode(
        f"{settings.api_key_id}:{settings.api_key_secret}".encode()
    ).decode()
    request = urllib.request.Request(
        settings.endpoint,
        headers={
            "Accept": "text/plain",
            "Authorization": f"Basic {token}",
            "User-Agent": "pg-cdc-lab/1",
        },
    )
    with urllib.request.urlopen(request, timeout=settings.timeout_seconds) as response:
        return response.read().decode("utf-8")


def _metric_families(payload: str) -> dict[str, list[Any]]:
    families: dict[str, list[Any]] = {}
    for family in text_string_to_metric_families(payload):
        families.setdefault(family.name, []).extend(family.samples)
    return families


def _select_pipe(
    families: dict[str, list[Any]], clickpipe_id: str | None
) -> dict[str, str]:
    candidates: dict[str, dict[str, str]] = {}
    for samples in families.values():
        for sample in samples:
            candidate_id = sample.labels.get("clickpipe_id")
            if candidate_id:
                candidates.setdefault(candidate_id, {}).update(sample.labels)

    if clickpipe_id:
        if clickpipe_id not in candidates:
            raise ValueError(
                f"ClickPipe {clickpipe_id!r} was not present in the Cloud metrics response"
            )
        return candidates[clickpipe_id]
    if len(candidates) == 1:
        return next(iter(candidates.values()))
    if not candidates:
        raise ValueError(
            "No ClickPipe metrics were present in the Cloud metrics response"
        )
    names = sorted(
        f"{labels.get('clickpipe_name', 'unknown')} ({candidate_id})"
        for candidate_id, labels in candidates.items()
    )
    raise ValueError(
        "Multiple ClickPipes were present; set CLICKPIPE_ID to one of: "
        + ", ".join(names)
    )


def _sample_value(
    families: dict[str, list[Any]],
    family_name: str,
    label_name: str,
    label_value: str,
) -> float | int | None:
    for sample in families.get(family_name, []):
        if sample.labels.get(label_name) == label_value:
            value = float(sample.value)
            return int(value) if value.is_integer() else value
    return None


def parse_clickpipe_metrics(
    payload: str, clickpipe_id: str | None = None
) -> dict[str, Any]:
    families = _metric_families(payload)
    labels = _select_pipe(families, clickpipe_id)
    selected_id = labels["clickpipe_id"]
    service_id = labels.get("clickhouse_service", "")
    record: dict[str, Any] = {
        "clickhouse_service_id": service_id,
        "clickhouse_service_name": labels.get("clickhouse_service_name", ""),
        "clickpipe_id": selected_id,
        "clickpipe_name": labels.get("clickpipe_name", ""),
        "clickpipe_source": labels.get("clickpipe_source", ""),
    }

    for sample in families.get("ClickPipes_Info", []):
        if sample.labels.get("clickpipe_id") == selected_id:
            record["clickpipe_status"] = sample.labels.get("clickpipe_status", "")
            break

    for family_name, field in PIPE_METRICS.items():
        record[field] = _sample_value(
            families, family_name, "clickpipe_id", selected_id
        )
    for family_name, field in SERVICE_METRICS.items():
        record[field] = _sample_value(
            families, family_name, "clickhouse_service", service_id
        )
    return record


def scrape_clickpipe_metrics(settings: CloudMetricsSettings) -> dict[str, Any]:
    return parse_clickpipe_metrics(
        fetch_prometheus_text(settings), settings.clickpipe_id
    )
