#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS]

set -e

NUM=${1:-500000}

cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
  --benchmark all --num "$NUM" --github-json \
  > benchmark-results.json

echo "Results written to benchmark-results.json" >&2
