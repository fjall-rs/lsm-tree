# Point-read key reconstruction benchmark

This benchmark establishes a baseline for the TODO above `Table::point_read`:
avoid constructing an owned user key when the caller only wants the value.
Run commands from the `lsm-tree` directory.

```sh
cargo bench --bench point_read --locked -- --save-baseline before-key-fuse
```

After implementing the optimization, compare against the saved baseline:

```sh
cargo bench --bench point_read --locked -- --baseline before-key-fuse
```

Do not run `--save-baseline before-key-fuse` after the fix: that overwrites the
original measurements. Keep `target/criterion` between runs. Criterion's HTML
report is at `target/criterion/report/index.html` with the default target directory.
Use the same machine, Rust toolchain, Cargo features, build flags, and workload;
avoid other CPU-heavy jobs while measuring. Repeat comparisons before drawing
conclusions from small differences.

## Workload

All cases use 256 sorted records, one version per key, a constant 13-byte value,
and 16-, 128-, or 512-byte keys sharing a long prefix. Query order is a fixed
permutation. There is no random-number generation or key allocation in the timed
loop. Each Criterion iteration performs one read, including dropping its result.
Results and inputs pass through `std::hint::black_box` to discourage elimination
of the measured work. Setup verifies the returned bytes before timing.

| Case | What is measured |
| --- | --- |
| `block/prefix_hit` | Lookup in a resident data block, returning a key stored as prefix plus suffix; currently calls `Slice::fused`. |
| `block/restart_hit` | Lookup of a restart entry, where the key is stored in full; no key fusion. |
| `block/miss` | An absent key within the block's key range; no result materialization. |
| `cached_tree/hit` | Public `get` through an empty memtable and flushed tables, with blocks warmed into the cache. |

The block uses restart interval 16 and no hash index. A restart entry stores a
full key so the reader can start decoding there; intervening entries reference
its prefix. Setup checks that these representations are actually present.
Both hit cases use the same block, but their search/scan work differs: subtracting
their times does **not** isolate the cost of `Slice::fused`. Compare each case
against its own pre-change baseline.

The tree uses 4 KiB target data blocks, restart interval 16, no data compression,
no data-block hash index, and a 16 MiB cache. It creates and flushes a temporary
tree, then verifies every query to warm the cache before timing. Its queries cover
both restart and prefix-compressed entries. The temporary directory is removed
after that key-length case finishes. The benchmark does not modify an existing
database.

## Comparing the fix

Keep `cached_tree/hit` unchanged: this measures the benefit through the same
public API before and after the fix. If the fix introduces a separate key-free
block lookup, update the measured block call to use it and update setup assertions
to verify the same values/misses. Otherwise the old full-entry block API may
correctly show no improvement. Keep datasets, query order and settings identical.

Expect the most direct effect in `block/prefix_hit`. Restart hits and misses are
controls for regressions, not a promise of exactly unchanged timings. Cached tree
reads show whether the saving remains meaningful alongside the rest of the read
path. No improvement is claimed until that comparison has been measured.

This is a focused CPU benchmark, not a general storage-engine benchmark. Cold
I/O, multiple versions, deletion markers, hash indexes, blob reads, and other
feature combinations need separate evaluation when the fix is implemented.
