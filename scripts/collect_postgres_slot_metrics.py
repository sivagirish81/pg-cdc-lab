#!/usr/bin/env python3
"""Sample PostgreSQL logical-slot progress to one well-formed CSV."""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv

FIELDS = [
    "observed_at",
    "slot_name",
    "active",
    "restart_lsn",
    "confirmed_flush_lsn",
    "current_wal_lsn",
    "retained_wal_bytes",
    "unconfirmed_wal_bytes",
    "wal_status",
    "safe_wal_size",
]

QUERY = """
SELECT
    clock_timestamp(),
    s.slot_name,
    s.active,
    s.restart_lsn::text,
    s.confirmed_flush_lsn::text,
    pg_current_wal_lsn()::text,
    pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
    pg_wal_lsn_diff(
        pg_current_wal_lsn(),
        s.confirmed_flush_lsn
    )::bigint,
    s.wal_status,
    s.safe_wal_size
FROM pg_replication_slots AS s
WHERE s.slot_type = 'logical'
  AND s.database = current_database()
  AND (%s IS NULL OR s.slot_name = %s)
ORDER BY s.slot_name
"""


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.getenv("PG_DSN"))
    parser.add_argument("--slot")
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("Set PG_DSN or pass --dsn")
    if args.interval <= 0:
        raise SystemExit("--interval must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with (
        psycopg.connect(args.dsn, autocommit=True) as connection,
        args.output.open("w", newline="", encoding="utf-8", buffering=1) as handle,
    ):
        writer = csv.writer(handle)
        writer.writerow(FIELDS)
        try:
            while True:
                with connection.cursor() as cursor:
                    cursor.execute(QUERY, (args.slot, args.slot))
                    for row in cursor.fetchall():
                        values = list(row)
                        values[0] = values[0].isoformat()
                        writer.writerow(values)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
