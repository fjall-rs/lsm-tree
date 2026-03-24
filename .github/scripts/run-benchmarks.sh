#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS] [ITERATIONS]

set -e

NUM=${1:-500000}
ITERATIONS=${2:-3}

cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
  --benchmark all --num "$NUM" --iterations "$ITERATIONS" --github-json \
  > benchmark-results.json

echo "Results written to benchmark-results.json" >&2
