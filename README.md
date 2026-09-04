# pg-cdc-lab

`pg-cdc-lab` is a reproducible, destination-adaptable PostgreSQL CDC transaction-boundary benchmark. Its first adapter targets ClickHouse Managed Postgres “Sync to ClickHouse”; the source workload, evidence format, correctness contracts, and offline analysis are product-generic.

The benchmark answers one question:

> How do large and long-running PostgreSQL transactions affect small-transaction commit-to-visible tail latency, logical-slot WAL retention, destination catch-up, and transaction atomicity?

It keeps four signals separate: PostgreSQL COMMIT acknowledgment, first destination query completion that observes a row, logical-slot/WAL progress, and manually recorded ClickPipe batch duration. `confirmed_flush_lsn` is consumer acknowledgment, not proof of ClickHouse visibility.

## Safety

A primary run writes roughly 600,000 small transactions during each ten-minute fixed window plus 1,000,000 large-transaction rows. WAL, indexes, protocol framing, and replicas make storage/network volume larger than logical payload. Use an isolated test database and ClickHouse database.

- High-volume runs require `--confirm-full-run`.
- `.env` and `results/` are gitignored; credentials are never copied into manifests.
- The harness stops on logical-slot `unreserved` or `lost` status and emits a warning when `safe_wal_size` crosses the configured threshold.
- It never triggers failover or deletes rows. Cleanup is an explicit SQL operation.
- Pressing Ctrl-C closes the elephant connection, causing PostgreSQL to roll back an open transaction, while retaining collected evidence.

## Install

Python 3.11 or later is recommended.

```bash
cd pg-cdc-lab
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

Populate `PG_DSN`. Characterize the source without ClickHouse credentials:

```bash
python cdc_lab.py source-preflight
```

Only after confirming this is an isolated test database, create the two source tables:

```bash
python cdc_lab.py setup-source
```

In ClickHouse Cloud, create “Sync to ClickHouse” for:

- `cdc_lab.cdc_probe_small`
- `cdc_lab.cdc_probe_large`

Then add the ClickHouse HTTPS query endpoint, database, user, password, and actual destination table names to `.env`. If more than one logical slot exists, set `CLICKPIPE_SLOT`.

To capture ClickPipe measurements automatically, also set `CLICKHOUSE_CLOUD_ORGANIZATION_ID`, `CLICKHOUSE_CLOUD_API_KEY_ID`, and `CLICKHOUSE_CLOUD_API_KEY_SECRET`. Set `CLICKPIPE_ID` when the organization exposes more than one pipe. These Cloud API credentials are distinct from the ClickHouse SQL username and password and remain only in the ignored `.env` file.

```bash
python cdc_lab.py preflight
```

The preflight must query both destination tables and resolve exactly one logical slot. When Cloud credentials are configured, it also resolves the ClickPipe and prints one redacted-safe metrics sample. A batch puller can be inactive between pulls; interpret activity together with LSN movement, ClickPipe state, and query visibility.

## Stage 0: environment characterization

Set the sanitized descriptors in `.env`: service sizes, regions, HA topology, configuration name, sync interval, and pull batch size. A run manifest also records PostgreSQL/ClickHouse versions, payload width, workers, polling intervals, `logical_decoding_work_mem`, and `max_slot_wal_keep_size`. Record autoscaling and operational events in `operator_notes.md`.

Observer query p50/p95/p99 is calculated from raw query durations. It must be materially faster than the CDC effect being resolved.

## Stage 1: smoke validation

Start Prometheus and Grafana, then run commit and rollback separately:

```bash
make observability-up
make smoke

python cdc_lab.py run \
  --outcome rollback \
  --rate 20 \
  --workers 4 \
  --baseline-seconds 30 \
  --large-rows 10000 \
  --hold-seconds 10 \
  --recovery-seconds 60
```

Proceed only when commit reaches 10,000 exact unique rows spanning 1–10,000, rollback remains zero at every sample, small committed/visible/exact-unique counts agree, slot samples are populated, no component reports an error, and `make report RESULTS=results/<run>` regenerates the same summary.

## Stage 2: load-only control

Use a warm-up followed by a measured ten-minute small-transaction control:

```bash
python cdc_lab.py run \
  --outcome commit \
  --load-only \
  --confirm-full-run \
  --warmup-seconds 120 \
  --baseline-seconds 600 \
  --rate 500 \
  --workers 32
```

This establishes the ordinary latency/WAL distribution, normal batch behavior, observer overhead, and whether the source generator sustains 500 TPS independently of an elephant transaction.

## Stage 3: primary comparison

```bash
python cdc_lab.py run \
  --outcome commit \
  --confirm-full-run \
  --rate 500 \
  --workers 32 \
  --baseline-seconds 600 \
  --large-rows 1000000 \
  --payload-bytes 256 \
  --hold-seconds 60 \
  --recovery-seconds 600
```

Repeat with `--outcome rollback`. First run one reconnaissance per outcome for `PG_CDC_LAB_CONFIGURATION=default_60s`, inspect the evidence, then repeat each validated comparison at least five times. Change the ClickPipe to ten seconds, set `PG_CDC_LAB_CONFIGURATION=latency_10s` and `PG_CDC_LAB_SYNC_INTERVAL_SECONDS=10`, and repeat with every other variable constant.

Do not begin another run until destination catch-up is complete, retained WAL returns to baseline, the ClickPipe shows no backlog, correctness passes, and the services return to a comparable idle state.

Based on Stage 3, vary only the dominant dimension: hold duration (0/60/300 seconds) at fixed size if WAL grows during the hold, or transaction size (100k/500k/1m rows) at fixed hold if post-commit drain dominates. A `cdc_v2` configuration is valid only on a path ClickHouse identifies as CDC v2.

## Measurement contract

Every row and metric is associated with one of:

```text
warmup → baseline → large_load → open_hold → outcome → post_outcome_drain → recovered
```

The offline analyzer classifies each small transaction by COMMIT acknowledgment time. Commit-to-visible latency is:

```text
completion time of first ClickHouse query observing row
− PostgreSQL COMMIT acknowledgment time
```

Both times use one process monotonic clock. The result is an upper bound containing up to one polling interval plus destination query execution. Raw observer duration is recorded separately.

WAL metrics are never conflated:

```text
retained_wal_bytes        = current WAL LSN − restart_lsn
unconfirmed_wal_bytes     = current WAL LSN − confirmed_flush_lsn
restart_to_confirmed_bytes = confirmed_flush_lsn − restart_lsn
```

Load validity defaults to `achieved_tps / requested_tps >= 0.95`; sustained queue growth also invalidates performance comparison. Recovery requires both rolling small-transaction p99 and retained-WAL p95 to be at most 1.20× their pre-transaction baselines for three consecutive windows. Configure these with `--load-fidelity-threshold`, `--recovery-threshold`, and `--recovery-window-seconds`.

Commit correctness requires exact count, `uniqExact(row_number)`, minimum 1, and expected maximum. Rollback requires zero visible rows at every observation. The final small-row query compares count with exact unique `(worker_id, seq)` tuples. Logical-slot loss and any surfaced component error fail correctness.

## Evidence and reports

Each run creates a timestamped `results/` directory containing raw commits, visibility observations, logical-slot samples, large visibility samples, exact small-row integrity, phase events, errors, a sanitized manifest, operator notes, ClickPipe batch transcription template, `summary.json`, `summary.md`, and `finding.md`.

```bash
make report RESULTS=results/<run>
make compare RESULTS='results/<run-1> results/<run-2>'
make evidence-bundle RESULTS='results/<run-1> results/<run-2>'
```

Comparison shows every independent run and reports median/range without treating transactions inside one run as independent repetitions. Invalid runs remain visible and are never silently discarded. The evidence bundle contains executive summary, finding, methodology, limitations, manifests, aggregate CSV/JSON, deterministic SVG/PNG charts, Grafana assets, Prometheus rules, raw results, and `sai-brief.md`. Unavailable measurements remain explicitly unavailable.

When Cloud API credentials are configured, the runner samples ClickPipe Prometheus metrics into `clickpipe_metrics.csv`: state, source slot lag, errors, records/bytes, CDC CPU, memory, and network receive. The CSV retains optional batch start/end/duration/rows columns for manual UI transcription because those batch boundaries are not exposed by the current Prometheus response. Batch duration includes pull, push, and waiting; do not call it pure processing time. Grafana is a live aid, never the source of truth.

## Live observability

```bash
make observability-up
# Grafana: http://localhost:3000 (admin / pg-cdc-lab)
# Prometheus: http://localhost:9090
make observability-down
```

If those host ports are occupied, use `GRAFANA_PORT=3300 PROMETHEUS_PORT=9190 make observability-up`.

The benchmark exposes bounded-cardinality `pg_cdc_lab_` metrics on port 9464 by default. Only `scenario`, `phase`, and `configuration` labels are used. The provisioned dashboard aligns phase, latency percentiles, achieved TPS, queue delay, three distinct WAL distances, spill/stream counters, large visibility, rollback leakage, and observer errors without misleading latency/WAL dual axes.

## Validation

```bash
make validate
```

This runs unit/synthetic integration tests, Python syntax checks, dashboard JSON validation, and Docker Compose validation. It does not claim an end-to-end test; only a run using real PostgreSQL, ClickHouse, and an active CDC path can do that.

## Explicit cleanup

After ClickPipe fully catches up and evidence is copied, issue deletes manually for the chosen run ID. Deletes generate more WAL and are replicated.

```sql
DELETE FROM cdc_lab.cdc_probe_small WHERE run_id = '<run-id>';
DELETE FROM cdc_lab.cdc_probe_large WHERE run_id = '<run-id>';
```
