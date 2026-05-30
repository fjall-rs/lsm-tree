#!/usr/bin/env bash
# Two-phase benchmark runner.
#
# Phase 1: populate the source DB in a container with no memory limit (mirrors
# the RocksDB PR's "Build the source DB once, unrestricted memory" step).
#
# Phase 2: for each config permutation, run the workload in a *fresh* container
# with the memory cgroup. Each container exit drops its share of the kernel page
# cache, so configs measured later don't inherit pollution from earlier ones.
set -euo pipefail

IMAGE=${IMAGE:-lsm-tree-bench:latest}
VOL_NAME=${VOL_NAME:-lsm-tree-bench-data}
# 1 GiB cgroup matches the RocksDB PR. The container's working memory (process
# + memtables + block cache + scratch FS pages) sits comfortably under this
# while still putting cache pressure on a multi-GiB DB.
MEMORY=${MEMORY:-1g}

# Workload knobs. Defaults are tuned for a 1 GiB cgroup with a 200 MiB DB and
# 25 MB/s write throttle — high enough to drive continuous compaction without
# running the cgroup out of headroom for the bench's working memory.
export LSMT_DIO_TOTAL=${LSMT_DIO_TOTAL:-50000}
export LSMT_DIO_NUM=${LSMT_DIO_NUM:-1000}
export LSMT_DIO_VALUE_SIZE=${LSMT_DIO_VALUE_SIZE:-4096}
export LSMT_DIO_DURATION=${LSMT_DIO_DURATION:-30}
export LSMT_DIO_WARMUP=${LSMT_DIO_WARMUP:-5}
export LSMT_DIO_TARGET_SIZE=${LSMT_DIO_TARGET_SIZE:-4194304}
export LSMT_DIO_WRITE_BPS=${LSMT_DIO_WRITE_BPS:-26214400}

echo ">>> Phase 1: populate (no memory limit)"
docker volume rm "${VOL_NAME}" >/dev/null 2>&1 || true
docker volume create "${VOL_NAME}" >/dev/null

docker run --rm \
  -v "${VOL_NAME}":/data \
  -e LSMT_DIO_MODE=populate \
  -e LSMT_DIO_WORKDIR=/data \
  -e LSMT_DIO_TOTAL \
  -e LSMT_DIO_VALUE_SIZE \
  -e LSMT_DIO_TARGET_SIZE \
  "${IMAGE}"

echo
echo ">>> Phase 2: run (memory limit ${MEMORY}, one container per config)"

declare -a CONFIGS=(buffered writes_only reads_only both)
RESULTS_DIR=$(mktemp -d)
for cfg in "${CONFIGS[@]}"; do
    echo
    echo "----- $cfg -----"
    docker run --rm \
      --memory="${MEMORY}" \
      --memory-swap="${MEMORY}" \
      -v "${VOL_NAME}":/data \
      -e LSMT_DIO_MODE=run \
      -e LSMT_DIO_CONFIG="${cfg}" \
      -e LSMT_DIO_WORKDIR=/data \
      -e LSMT_DIO_TOTAL \
      -e LSMT_DIO_NUM \
      -e LSMT_DIO_VALUE_SIZE \
      -e LSMT_DIO_DURATION \
      -e LSMT_DIO_WARMUP \
      -e LSMT_DIO_TARGET_SIZE \
      -e LSMT_DIO_WRITE_BPS \
      "${IMAGE}" 2>&1 | tee "${RESULTS_DIR}/${cfg}.log"
done

echo
echo ">>> Aggregated results"
{
    printf '%-14s  %11s  %9s  %9s  %10s  %11s\n' \
        "config" "throughput" "P50 (us)" "P99 (us)" "P99.9 (us)" "P99.99 (us)"
    printf '%s\n' "------------------------------------------------------------------------------"
    for cfg in "${CONFIGS[@]}"; do
        grep '^JSON: ' "${RESULTS_DIR}/${cfg}.log" \
            | sed 's/^JSON: //' \
            | python3 -c '
import json, sys
data = json.loads(sys.stdin.read())
for r in data:
    print(f"{r[\"config\"]:<14}  {r[\"throughput\"]:>11.0f}  "
          f"{r[\"p50_us\"]:>9.2f}  {r[\"p99_us\"]:>9.2f}  "
          f"{r[\"p999_us\"]:>10.1f}  {r[\"p9999_us\"]:>11.1f}")
'
    done
}

docker volume rm "${VOL_NAME}" >/dev/null 2>&1 || true
rm -rf "${RESULTS_DIR}"
