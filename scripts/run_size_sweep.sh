#!/usr/bin/env bash
set -euo pipefail

# This deliberately pauses between runs so the operator can verify destination
# catch-up, slot lag, and comparable idle state. Invoke with:
#   REPEATS=5 bash scripts/run_size_sweep.sh

repeats="${REPEATS:-5}"
sizes=(10000 50000 100000 250000 500000 1000000)

for rows in "${sizes[@]}"; do
  for ((repeat = 1; repeat <= repeats; repeat++)); do
    printf 'Starting rows=%s repeat=%s/%s\n' "$rows" "$repeat" "$repeats"
    python cdc_lab.py run \
      --outcome commit \
      --confirm-full-run \
      --rate "${SMALL_TXN_RATE:-500}" \
      --workers "${WORKERS:-32}" \
      --baseline-seconds "${BASELINE_SECONDS:-120}" \
      --large-rows "$rows" \
      --payload-bytes "${PAYLOAD_BYTES:-256}" \
      --hold-seconds "${HOLD_SECONDS:-0}" \
      --recovery-seconds "${RECOVERY_SECONDS:-120}" \
      --slot-poll-seconds "${SLOT_POLL_SECONDS:-0.25}"

    printf '%s\n' \
      'Verify destination catch-up, slot lag, and idle state before continuing.'
    read -r -p 'Press Enter for the next independent run (Ctrl-C to stop): '
  done
done
