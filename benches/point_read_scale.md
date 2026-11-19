# Point reads at MiB–GiB scale

Run from `lsm-tree`:

```sh
cargo bench --bench point_read_scale --locked -- --save-baseline scale-before
# After the implementation change, with identical saved datasets:
cargo bench --bench point_read_scale --locked -- --baseline scale-before
```

The benchmark creates six reusable datasets under the ignored directory
`.bench/point-read-scale` (override with `LSM_SCALE_DATA`). Each holds 4 MiB,
64 MiB, or 1 GiB of **value bytes**, plus 128- or 512-byte keys. Values are 1 KiB,
so record counts are 4,096, 65,536, and 1,048,576. Logical size includes both key
and value bytes; actual table size and table count are printed as `DATASET` lines.
The largest datasets exceed 1 GiB on disk because compression is disabled.

Keys are sorted by integer ID; groups of 256 share a long prefix. Sorted bulk
ingestion writes tables directly, avoiding a GiB-sized memtable. It uses the
same table writer as flush/compaction. Each dataset is ingested once, then reused
unchanged across implementation versions. Ingestion starts at level 1 and splits
output at the ingestion writer's 64 MiB target. There are no overlapping versions,
updates, tombstones, concurrent writers, or background compaction in this workload.

Each dataset is tested with a 2 GiB cache (`resident`) and a 64 MiB cache
(`cache64MiB`). Both use 4 KiB target data blocks, restart interval 16, no data-block
hash index, and the default Bloom-filter/index configuration. All records are read
and verified before timing each case. The resident cache can retain the complete
dataset; the bounded cache cannot retain the largest one. OS file pages are warm:
cache misses do not establish physical-device cold-I/O latency. At the smaller
sizes, the 64 MiB cache may also fit the complete dataset.

Each timed iteration generates a deterministic ID by an odd-multiplier permutation
of the complete power-of-two ID space, updates 16 bytes in a reusable key buffer,
and performs one successful public `get`. All IDs are visited before the sequence
repeats. No query-key allocation occurs in the harness's timed loop. Lookup results
are consumed with `black_box` and dropped. Key preparation is included in both
implementations. One version per key and a fixed 1 KiB value isolate read-path
behavior while producing genuinely large table files.

There are 12 cases: 3 sizes × 2 key lengths × 2 cache capacities. Each uses 100
samples, 2 seconds of warm-up, and approximately 5 seconds of measurement.

For reliable comparisons, build both executables before measuring; run them
sequentially with the same Cargo.lock, Rust toolchain, features and datasets. Pin
to the same CPU and avoid competing workloads. Repeat in reverse implementation
order to check for drift. Pinning does not lock CPU frequency or isolate its SMT
sibling. Save raw measurements and environment details under ignored
`benches/results/`, rather than committing measurements to source control.

Build separate source revisions with **separate target directories**. Before
running a comparison, check the captured executables with `sha256sum`: identical
binaries invalidate the experiment. For this specific optimization, `nm -C` must
show `get_point_result` and `find_point_record` only in the fixed executable, not
in the original baseline. Archive both binary hashes with the measurements.
A previous run accidentally captured the same executable from a shared target
directory for both revisions; its timing differences must not be interpreted as
an implementation effect.
