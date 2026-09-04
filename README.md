# pg-cdc-lab

`pg-cdc-lab` is a reproducible PostgreSQL CDC transaction-boundary benchmark. Its first adapter measures ClickHouse Managed Postgres through ClickPipes CDC / PeerDB into ClickHouse, while its source workload, evidence format, correctness contracts, and offline analysis remain product-generic.

Architecture under test:

```text
ClickHouse Managed Postgres
  -> PostgreSQL logical CDC
  -> ClickPipes CDC / PeerDB raw table
  -> raw-to-final processing
  -> ClickHouse destination table
```

The lab asks two related questions:

1. For a large atomic Postgres transaction, where does observed commit-to-destination-sync latency appear to be spent?
2. Does that transaction create latency, head-of-line blocking, or backpressure for unrelated small transactions?

It keeps four signals separate: PostgreSQL COMMIT acknowledgment, first destination query completion that observes a row, logical-slot/WAL progress, and manually recorded ClickPipe batch duration. `confirmed_flush_lsn` is consumer acknowledgment, not proof of ClickHouse visibility.

## One observed baseline run

A 500,000-row atomic transaction produced **9.968626 s observed commit-to-destination-sync-timestamp latency** in one Managed Postgres trial run. The first raw CDC timestamp appeared after 0.465411 s; the 500,000 raw timestamps then spanned 7.720470 s in one PeerDB batch. ClickHouse `system.query_log` recorded a 1,197 ms raw-table INSERT, a 93.829 ms raw-to-final gap, and a 653 ms final INSERT.

This is one observed run, not a normal, expected, or universal ClickPipes latency. The raw timestamp span is an observed source/capture-path span; it does not isolate logical decoding, PeerDB, or network time. The final ClickHouse INSERT was not the dominant part of this particular interval. See `results/examples/baseline-500k/metrics.json` for the sanitized source-of-truth fixture.

## Safety

A primary run writes roughly 600,000 small transactions during each ten-minute fixed window plus 1,000,000 large-transaction rows. WAL, indexes, protocol framing, and replicas make storage/network volume larger than logical payload. Use an isolated test database and ClickHouse database.

- High-volume runs require `--confirm-full-run`.
- `.env` and `results/` are gitignored; credentials are never copied into manifests.
- The harness stops on logical-slot `unreserved` or `lost` status and emits a warning when `safe_wal_size` crosses the configured threshold.
- It never triggers failover or deletes rows. Cleanup is an explicit SQL operation.
- Pressing Ctrl-C closes the large-transaction connection, causing PostgreSQL to roll back an open transaction, while retaining collected evidence.

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

The equivalent manual setup is:

```bash
psql "$PG_DSN" -v ON_ERROR_STOP=1 -f sql/postgres/setup.sql
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

## Reproduction modes

### Mode A: automated benchmark driver

Use the driver for repeated workloads, transaction-size sweeps, and the concurrent small-transaction experiment. It generates a unique run ID, writes the large rows in one transaction, acknowledges COMMIT once, samples the logical slot and destination, and writes a sanitized run manifest.

```bash
python cdc_lab.py run \
  --outcome commit \
  --confirm-full-run \
  --rate 500 \
  --workers 32 \
  --baseline-seconds 120 \
  --large-rows 500000 \
  --payload-bytes 256 \
  --hold-seconds 0 \
  --recovery-seconds 120 \
  --slot-poll-seconds 0.25
```

Use `--rate 0` only after adding a load-disable mode; the current driver expects a positive rate. For a large-transaction-only control today, use the manual path.

For a size sweep, run independent repetitions at `10000`, `50000`, `100000`, `250000`, `500000`, and `1000000` rows. Use at least five repeats before treating p50/p95 as population summaries, and keep service configuration constant.

```bash
REPEATS=5 bash scripts/run_size_sweep.sh
```

The sweep pauses after every run. Continue only after destination catch-up, logical-slot lag, and service idle state have returned to a comparable baseline.

### Mode B: manual `psql` control path

> **When do I need `psql`?** Use this path when you need exact interactive control over the transaction boundary, want to hold a transaction open, need a second terminal sampling WAL / the logical slot before COMMIT, or direct Managed Postgres access through `psql` is easier than running the benchmark driver.

For a single atomic transaction that commits immediately:

```bash
RUN_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
psql "$PG_DSN" -v ON_ERROR_STOP=1 \
  -v run_id="$RUN_ID" \
  -v transaction_rows=500000 \
  -v payload_bytes=256 \
  -f sql/postgres/large_transaction.sql
```

For an intentional pause, open Terminal A and run:

```bash
psql "$PG_DSN" -v ON_ERROR_STOP=1
```

Then copy `BEGIN` through the `INSERT` from `sql/postgres/large_transaction.sql`, substituting a unique run ID, but do not issue `COMMIT`. In Terminal B, start slot sampling **before COMMIT**:

```bash
mkdir -p "results/$RUN_ID"
python scripts/collect_postgres_slot_metrics.py \
  --interval 0.25 \
  --output "results/$RUN_ID/slot_samples_manual.csv"
```

Return to Terminal A, issue `COMMIT`, then immediately run:

```bash
psql "$PG_DSN" -v ON_ERROR_STOP=1 \
  -f sql/postgres/post_commit_lsn.sql
```

The returned `commit_observed_at` is an observation immediately after COMMIT completes, not an exact server-internal commit timestamp. Prefer the automated driver's COMMIT acknowledgment marker when available.

### Manual ClickHouse / PeerDB inspection

`psql` is for the source transaction and WAL / slot inspection. ClickHouse SQL is for the destination table, PeerDB raw table, and `system.query_log`. Run the files under `sql/clickhouse/` in the ClickHouse Cloud SQL console, with `clickhouse-client`, or through another ClickHouse query helper.

The SQL files use `{{destination_table}}` and `{{raw_table}}` for validated identifier substitution and ClickHouse `{name:Type}` query parameters for values. Do not silently choose the first raw table if discovery returns more than one; verify which candidate contains the run ID.

Automated canonical extraction:

```bash
python peerdb_metrics.py \
  --run-id "$RUN_ID" \
  --commit-ts '2026-09-04T07:37:57.350374Z' \
  --destination-table cdc_lab_cdc_probe_large \
  --transaction-rows 500000
```

Pass `--raw-table default._peerdb_raw_...` when discovery cannot uniquely resolve one table. The collector writes `metrics.json`, `metrics.csv`, and `events.json` under `results/$RUN_ID/`. Missing query-log stages stay null; they are never represented as 1970 epoch values.

Regenerate the checked-in baseline charts without live credentials:

```bash
python plot_peerdb_results.py \
  results/examples/baseline-500k \
  --output charts/peerdb
```

Generate the transaction-size chart from several canonical runs:

```bash
python plot_peerdb_results.py results/<run-1> results/<run-2> results/<run-3>
```

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

This establishes the ordinary latency/WAL distribution, normal batch behavior, observer overhead, and whether the source generator sustains 500 TPS independently of a large transaction.

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

## Transaction-size and interference experiments

The canonical PeerDB chart path accepts any number of `metrics.json` artifacts and groups them by `transaction_rows`. The intended sweep is 10K, 50K, 100K, 250K, 500K, and 1M rows with five independent repeats per size. It plots commit-to-first-raw, raw CDC timestamp span, final INSERT duration, and commit-to-sync as overlapping measurements; it does not stack them as sequential stages.

The automated driver already runs the concurrent interference experiment: `--rate 500` maintains the small-transaction workload before, during, and after the large transaction. `summary.json` reports small-transaction p50/p95/p99 by commit phase. The relevant comparison windows are `baseline`, `large_load` / `open_hold`, and `post_outcome_drain`. Runs that fail load fidelity or correctness remain visible and are not valid performance comparisons.

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

### PeerDB metric semantics

- `_peerdb_synced_at` is destination sync / batch timestamp metadata. One shared value does not mean ClickHouse physically inserted all rows in zero time.
- `_peerdb_timestamp` supports raw CDC ordering and relative timing. Its first-to-last span does not identify exact logical-decoding, PeerDB, or network time.
- `system.query_log.query_duration_ms` measures ClickHouse query execution. Query it through `clusterAllReplicas('default', system.query_log)` so a Cloud replica boundary does not hide an operation.
- `intermediate_processing_ms` is the wall-clock gap between raw INSERT completion and final INSERT start. It can include orchestration, setup, and intermediate queries; it is not labeled entirely as normalization.
- The event timeline uses actual timestamps where available. It does not add the raw CDC timestamp span to ClickHouse query durations when those intervals may overlap.

## Evidence and reports

Each run creates a timestamped `results/` directory containing raw commits, visibility observations, logical-slot samples, large visibility samples, exact small-row integrity, phase events, errors, a sanitized manifest, operator notes, ClickPipe metrics with optional batch-transcription columns, `summary.json`, `summary.md`, and `finding.md`.

```bash
make report RESULTS=results/<run>
make compare RESULTS='results/<run-1> results/<run-2>'
make evidence-bundle RESULTS='results/<run-1> results/<run-2>'
```

Comparison shows every independent run and reports median/range without treating transactions inside one run as independent repetitions. Invalid runs remain visible and are never silently discarded. The evidence bundle contains executive summary, finding, methodology, limitations, manifests, aggregate CSV/JSON, deterministic SVG/PNG charts, Grafana assets, Prometheus rules, raw results, and `sai-brief.md`. Unavailable measurements remain explicitly unavailable.

When Cloud API credentials are configured, the runner samples ClickPipe Prometheus metrics into `clickpipe_metrics.csv`: state, source slot lag, errors, records/bytes, CDC CPU, memory, and network receive. The CSV retains optional batch start/end/duration/rows columns for manual UI transcription because those batch boundaries are not exposed by the current Prometheus response. Batch duration includes pull, push, and waiting; do not call it pure processing time. Grafana is a live aid, never the source of truth.

## Measured facts, hypotheses, and follow-up instrumentation

The checked-in 500K fixture supports only the measured values stated near the top of this README. It does not prove that PeerDB itself, Postgres logical decoding, or network transfer took 7.72 seconds; it does not establish fixed ClickPipes throughput or latency; and it does not show that this Managed Postgres path was using CDC V2 streamed transactions.

The next source-side measurement is the 100–250 ms logical-slot sample in `sql/postgres/replication_slot_sample.sql`, aligned with the post-COMMIT WAL marker. The next workload measurement is the existing 500 TPS small-transaction stream around the large transaction. Together, they can narrow source/WAL-consumer progress and test unrelated-transaction interference without claiming an internal bottleneck from timestamps alone.

Open questions for streamed / ongoing transaction support:

- Do slot spill/stream counters move before COMMIT while destination atomicity remains intact?
- Does streaming reduce post-COMMIT raw CDC span or WAL retention?
- Does a large transaction affect unrelated small transactions before or only after COMMIT?
- Which behavior is specific to the deployed ClickPipes / CDC version and configuration?

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
