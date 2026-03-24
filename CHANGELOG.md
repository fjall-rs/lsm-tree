# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Fork:** [structured-world/coordinode-lsm-tree](https://github.com/structured-world/coordinode-lsm-tree),
> a maintained fork of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree).
> Fork releases use `v`-prefixed tags (`v4.0.0`); upstream uses bare tags (`3.1.2`).

## [Unreleased]

## [4.0.0] — Fork Epoch (2026-03-23)

First release of `coordinode-lsm-tree` — maintained fork of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree) v3.1.1.
Published to [crates.io](https://crates.io/crates/coordinode-lsm-tree). All changes since upstream v3.1.1.

### Added

- Merge operators for commutative LSM operations (#28)
- Range tombstones (delete_range / delete_prefix) with V4 disk format (#21)
- Block-level encryption at rest (AES-256-GCM) (#71)
- Custom key comparison / UserComparator (#67)
- Prefix bloom filters for graph key encoding (#43, #64, #68, #70)
- Arena-based skiplist for memtable (#79)
- Fs trait for pluggable filesystem backends (#80, #109, #107, #112)
- Zstd compression support
- SequenceNumberGenerator trait (#10)
- multi_get() for batch point reads (#9)
- verify_integrity() for full-file checksum verification (#4)
- Intra-L0 compaction for overlapping runs (#5)
- Optimized contains_prefix() method (#6)
- Size-tiered, dynamic leveling, and multi-level compaction strategies (#66)
- db_bench benchmark suite (#45)
- Per-source RT visibility in range/prefix iteration
- Write-side size cap enforcement
- Seqno-aware seek for iterator bounds

### Fixed

- Resolve L0 stale reads when optimize_runs reorders SSTs (#56)
- Select highest-seqno entry across all L0 tables (#54)
- Cursor wrap on exact block fill corrupts arena (#130)
- Thread UserComparator through Run, KeyRange, and Version methods (#117)
- Preserve range tombstones covering gaps between output tables (#137)
- Scanner should not treat corrupted magic matching META as EOF (#63)
- Replace panic paths in vlog Metadata::from_slice with Result (#62)
- Decompression buffer validation (#7)
- V4 blob frame header checksum (#44)
- 100+ correctness fixes for range tombstones, compaction, MVCC

### Performance

- Partition-aware bloom filtering for point-read pipeline (#102)
- Lazy iterator pipeline initialization for point reads (#110)
- Replace OsRng with thread-local seeded CSPRNG (#104)
- Reduce allocations in encrypt/decrypt block pipeline (#105)
- Optimize range tombstone lookup in table-skip and point-read (#55)
- Seqno-aware seek in data block point reads (#8)
- Compute L2 overlaps per-range in multi-level compaction (#108)
- Unify merge resolution via bloom-filtered iterator pipeline (#69)

### Refactored

- Centralize OwnedIndexBlockIter adapter pattern (#99)
- Return CompactionResult from Tree::compact (#103)
- Thread Fs through FileAccessor, DescriptorTable, table::Writer, BlobFile (#107, #112)
- Seal AbstractTree internals
- Replace Mutex with RwLock for range tombstone concurrency
- Add #[non_exhaustive] to CompressionType enum

### Testing

- 43 new test suites: property-based oracle, custom comparator, encryption, corruption, concurrency
- Integration tests for compaction/merge with custom comparator (#100)
- BTreeMap oracle with multi-byte prefix keys (#65)
- End-to-end corruption test for seqno metadata (#96)
