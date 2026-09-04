"""Destination adapter boundary; ClickHouse is the first implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class DestinationAdapter(Protocol):
    def preflight(self) -> dict[str, Any]: ...
    def small_rows_after(
        self, run_id: str, highwater: dict[int, int]
    ) -> list[tuple[int, int]]: ...
    def large_state(self, run_id: str) -> tuple[int, int, int | None, int | None]: ...
    def small_integrity(self, run_id: str) -> tuple[int, int]: ...
    def close(self) -> None: ...


def _identifier(value: str) -> str:
    import re

    parts = value.split(".")
    if len(parts) not in (1, 2) or not all(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts
    ):
        raise ValueError(f"Unsafe ClickHouse table name: {value!r}")
    return ".".join(f"`{part}`" for part in parts)


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool
    small_table: str
    large_table: str


class ClickHouseDestination:
    def __init__(self, settings: ClickHouseSettings):
        import clickhouse_connect

        self.settings = settings
        self.client = clickhouse_connect.get_client(
            host=settings.host,
            port=settings.port,
            username=settings.username,
            password=settings.password,
            database=settings.database,
            secure=settings.secure,
            query_limit=0,
        )
        self.small = _identifier(settings.small_table)
        self.large = _identifier(settings.large_table)

    def preflight(self) -> dict[str, Any]:
        version = self.client.query("SELECT version(), now64(3)").result_rows[0]
        return {
            "adapter": "clickhouse",
            "server_version": str(version[0]),
            "destination_time": str(version[1]),
            "small_table": self.settings.small_table,
            "small_table_rows": int(
                self.client.query(f"SELECT count() FROM {self.small}").result_rows[0][0]
            ),
            "large_table": self.settings.large_table,
            "large_table_rows": int(
                self.client.query(f"SELECT count() FROM {self.large}").result_rows[0][0]
            ),
        }

    def small_rows_after(
        self, run_id: str, highwater: dict[int, int]
    ) -> list[tuple[int, int]]:
        predicates = " OR ".join(
            f"(worker_id = {int(worker)} AND seq > {int(seq)})"
            for worker, seq in highwater.items()
        )
        query = (
            f"SELECT worker_id, seq FROM {self.small} WHERE run_id = toUUID('{run_id}') "
            f"AND ({predicates}) ORDER BY worker_id, seq SETTINGS use_query_cache = 0"
        )
        return [(int(a), int(b)) for a, b in self.client.query(query).result_rows]

    def large_state(self, run_id: str) -> tuple[int, int, int | None, int | None]:
        query = (
            "SELECT count(), uniqExact(row_number), minOrNull(row_number), maxOrNull(row_number) "
            f"FROM {self.large} WHERE run_id = toUUID('{run_id}') SETTINGS use_query_cache = 0"
        )
        row = self.client.query(query).result_rows[0]
        return int(row[0]), int(row[1]), row[2], row[3]

    def small_integrity(self, run_id: str) -> tuple[int, int]:
        row = self.client.query(
            f"SELECT count(), uniqExact(tuple(worker_id, seq)) FROM {self.small} "
            f"WHERE run_id = toUUID('{run_id}') SETTINGS use_query_cache = 0"
        ).result_rows[0]
        return int(row[0]), int(row[1])

    def close(self) -> None:
        self.client.close()
