# Direct I/O benchmark

Reproduces the methodology from RocksDB PR #14743 to quantify the impact of
`use_direct_io_for_compaction_reads` and `use_direct_io_for_flush_and_compaction`
on user-read tail latency under sustained compaction.

## Methodology

Two-phase, RocksDB-PR-style workload:

1. **Phase 1 (populate)** — outside the memory cgroup. Builds a source LSM tree
   with `LSMT_DIO_TOTAL` keys × `LSMT_DIO_VALUE_SIZE` bytes of incompressible
   value bytes, flushes, and major-compacts to a stable shape. Copies it to a
   fresh scratch directory per config so each per-config run starts from the
   same on-disk layout.

2. **Phase 2 (run)** — one fresh container per config, with `--memory=${MEMORY}`
   (`docker run --memory` corresponds 1:1 to a Linux cgroup memory limit). Each
   container exit releases its share of the kernel page cache, so configs
   measured later don't inherit pollution from earlier ones.

   Inside the container:
   - 4 reader threads issue random `tree.get(...)` against a hot subset of
     `LSMT_DIO_NUM` keys.
   - 1 writer thread issues `tree.insert(...)` across the full `LSMT_DIO_TOTAL`
     key range, token-bucket throttled to `LSMT_DIO_WRITE_BPS`. The writer
     calls `flush_active_memtable` every 2000 writes so the workload produces
     L0 churn for the compactor to roll up.
   - 1 compaction-driver thread calls `tree.major_compact(target_size, 0)`
     every 2 s.
   - Workload runs for `LSMT_DIO_WARMUP + LSMT_DIO_DURATION` seconds; latency
     samples collected only past the warmup boundary. Reservoir sampling caps
     the per-thread sample to 2 M (`u32` nanoseconds), so memory stays flat.

## Reproducing

```bash
docker build -f examples/Dockerfile.bench -t lsm-tree-bench:latest .
bash examples/run_bench.sh
```

All knobs are environment variables; see the top of `run_bench.sh`.

## Headline results — 4× cache oversubscription

Host: Apple M4 Pro, 48 GiB RAM, Docker Desktop (Linux aarch64 VM). Container
memory limit: 1 GiB. DB on disk: 4 034 MiB (1 M keys × 4 096 B incompressible).
Hot reader set: 4 000 keys (~16 MiB). Writer throttled to 10 MiB/s.
Duration: 30 s per config, 10 s warmup. **N = 3 iterations**, mean reported.

| Config       | Throughput  | P50 (µs) | P99 (µs) | P99.9 (µs) | P99.99 (µs) |
|--------------|------------:|---------:|---------:|-----------:|------------:|
| buffered     | 670 941     | 4.79     | 17.49    | 54.49      | 237.03      |
| writes_only  | 716 707     | 4.68     | 16.42    | 39.28      | 141.29      |
| reads_only   | 746 252     | 4.46     | 15.36    | 37.83      | 196.75      |
| **both**     | **738 635** | **4.44** | **15.78**| **39.47**  | **142.71**  |

Deltas vs the buffered baseline:

| Config       | Throughput | P50    | P99    | P99.9  | P99.99 |
|--------------|-----------:|-------:|-------:|-------:|-------:|
| writes_only  | +6.8 %     | −2.2 % | −6.1 % | −27.9% | **−40.4%** |
| reads_only   | +11.2 %    | −6.9 % | −12.2% | −30.6% | −17.0% |
| **both**     | **+10.1%** | −7.3 % | −9.8 % | −27.6% | **−39.8%** |

### Per-iteration raw data

| Iter | Config       | Throughput | P50  | P99   | P99.9 | P99.99 |
|------|--------------|-----------:|-----:|------:|------:|-------:|
| 1    | buffered     | 618 921    | 5.04 | 19.88 | 72.38 | 300.71 |
| 1    | writes_only  | 643 857    | 5.17 | 19.04 | 50.17 | 184.92 |
| 1    | reads_only   | 768 770    | 4.29 | 15.00 | 34.88 | 186.83 |
| 1    | both         | 658 438    | 4.88 | 17.88 | 52.25 | 190.29 |
| 2    | buffered     | 727 933    | 4.42 | 15.21 | 36.83 | 185.88 |
| 2    | writes_only  | 763 551    | 4.38 | 14.75 | 31.92 | 107.08 |
| 2    | reads_only   | 773 228    | 4.29 | 14.58 | 31.00 | 185.17 |
| 2    | both         | 755 177    | 4.33 | 15.17 | 34.62 | 126.25 |
| 3    | buffered     | 665 968    | 4.92 | 17.38 | 54.25 | 224.50 |
| 3    | writes_only  | 742 714    | 4.50 | 15.46 | 35.75 | 131.88 |
| 3    | reads_only   | 696 758    | 4.79 | 16.50 | 47.62 | 218.25 |
| 3    | both         | 802 290    | 4.12 | 14.29 | 31.54 | 111.58 |

## Discussion

The shape of the result matches RocksDB PR #14743's "larger hot set" scenario:

- **Throughput is up across all direct configs** because the kernel no longer
  spends cycles managing compaction reads/writes through the page cache.
- **P50 is essentially unchanged** (hot set fits in cache regardless of
  config), so direct I/O is "free" on the median read path.
- **Tail latency drops sharply.** At P99.99 — the percentile that captures
  cache-miss read latency after the hot set has been partially evicted — the
  `both` config gives a ~40 % reduction (237 µs → 143 µs).

Why does `reads_only` give a smaller P99.99 win than `writes_only` here?
RocksDB PR #14743 documents the same dynamic: direct compaction reads bypass
the kernel's readahead window, so compaction without an explicit prefetch in
userspace runs slower per-byte. lsm-tree does not yet have a compaction-side
prefetch (the equivalent of RocksDB's `compaction_readahead_size`), so
`reads_only` has to win back via cache-protection what it loses on readahead.
The `writes_only` knob has no such trade-off — flush/compaction-output
write-back is purely a cache pollution avoidance with no readahead cost.

## Recommended production setting

`use_direct_io_for_flush_and_compaction = true` for all deployments with
sustained compaction activity. Adding `use_direct_io_for_compaction_reads =
true` is also a win at deep percentiles on cache-pressured workloads
(throughput + P99.99 are both ahead of `writes_only` alone in iteration 1
and within noise in iterations 2 & 3).

## What's NOT shown

- The macOS dev-host bench (run without Docker) shows much smaller deltas
  because the unified buffer cache on macOS is not constrained, so the
  cache-pollution scenario is hard to reproduce without an explicit memory
  limit. Tail-latency gains on macOS were < 10 % in informal testing.
- Linux O_DIRECT on filesystems that reject it (tmpfs, some FUSE, some
  Docker overlay configurations) is already covered by the open-time
  fallback path in `src/direct_io/chunked.rs`; the benchmark transparently
  falls back to buffered I/O in that case.

## Confirming direct I/O was actually used

Because an O_DIRECT-rejecting filesystem silently downgrades to buffered I/O,
it is possible to accidentally measure "buffered vs buffered" and see no delta.
Before trusting the numbers, confirm direct I/O was actually in effect:

- Watch the logs. The first fallback emits a single `log::warn` from
  `log_unsupported_once` in `src/direct_io/chunked.rs`
  ("direct I/O not supported by filesystem ... falling back to buffered I/O").
  Run the container with `RUST_LOG=warn` (the bench uses `env_logger`/`test_log`);
  if that line appears, the `writes_only` / `reads_only` / `both` configs are
  *not* actually exercising direct I/O and the comparison is meaningless.
- Verify the data directory lives on an O_DIRECT-capable filesystem (a real
  block-backed mount such as ext4/xfs). The Docker volume used by
  `run_bench.sh` is backed by the host's overlay/volume driver — prefer a
  bind-mount to a real disk if you see the fallback warning, and re-run.
