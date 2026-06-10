#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  cat <<'USAGE'
Usage:
  run_liquidity_pipeline.sh YYYY-MM-DD [input_path]

Arguments:
  YYYY-MM-DD   Snapshot date to replay
  input_path   Optional parquet directory or file path

If input_path is omitted, the script looks for:
  ./backfill_YYYYMMDD
USAGE
  exit 1
fi

SNAPSHOT_DATE="$1"
INPUT_PATH="${2:-}"

if [[ ! "$SNAPSHOT_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "error: snapshot date must use YYYY-MM-DD format" >&2
  exit 1
fi

SNAPSHOT_YYYYMMDD="${SNAPSHOT_DATE//-/}"
RUN_ID="liquidity-${SNAPSHOT_YYYYMMDD}"
PIPELINE_VERSION="${PIPELINE_VERSION:-1.0.0}"
DATASET_VERSION="${DATASET_VERSION:-demo-2026-01-01-v1}"
PROCESSING_TIMESTAMP_UTC="${SNAPSHOT_DATE}T23:59:59Z"

if [[ -z "$INPUT_PATH" ]]; then
  INPUT_PATH="./backfill_${SNAPSHOT_YYYYMMDD}"
fi

if [[ ! -e "$INPUT_PATH" ]]; then
  echo "error: input path does not exist: $INPUT_PATH" >&2
  exit 1
fi

python3 -m src.ingestion "$INPUT_PATH" \
  --run-id "$RUN_ID" \
  --pipeline-version "$PIPELINE_VERSION" \
  --dataset-version "$DATASET_VERSION" \
  --processing-timestamp-utc "$PROCESSING_TIMESTAMP_UTC"
