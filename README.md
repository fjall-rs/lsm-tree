<p align="center">
  <img src="/logo.png" height="160">
</p>

[![CI](https://github.com/fjall-rs/lsm-tree/actions/workflows/test.yml/badge.svg)](https://github.com/fjall-rs/lsm-tree/actions/workflows/test.yml)
[![docs.rs](https://img.shields.io/docsrs/lsm-tree?color=green)](https://docs.rs/lsm-tree)
[![Crates.io](https://img.shields.io/crates/v/lsm-tree?color=blue)](https://crates.io/crates/lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.91.0-blue)
[![dependency status](https://deps.rs/repo/github/fjall-rs/lsm-tree/status.svg)](https://deps.rs/repo/github/fjall-rs/lsm-tree)

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs) in Rust.

> [!NOTE]
> This crate only provides a primitive LSM-tree, not a full storage engine.
> For example, it does not ship with a write-ahead log.
> You probably want to use https://github.com/fjall-rs/fjall instead.

## About

This is the most feature-rich LSM-tree implementation in Rust! It features:

- Thread-safe `BTreeMap`-like API
- Mostly [safe](./UNSAFE.md) & 100% stable Rust
- Block-based tables with compression support & prefix truncation
  - Optional block hash indexes in data blocks for faster point lookups [[3]](#footnotes)
  - Per-level filter/index block pinning configuration
- Range & prefix searching with forward and reverse iteration
- Block caching to keep hot data in memory
- File descriptor caching with upper bound to reduce fopen calls
- *AMQ* filters (currently Bloom filters) to improve point lookup performance
- Multi-versioning of KVs, enabling snapshot reads
- Optionally partitioned block index & filters for better cache efficiency [[1]](#footnotes)
- Size-tiered, (concurrent) Leveled and FIFO compaction 
- Multi-threaded flushing (immutable/sealed memtables)
- Key-value separation (optional) [[2]](#footnotes)
- Single deletion tombstones ("weak" deletion)

Keys are limited to 65536 bytes, values are limited to 2^32 bytes.
As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.

## Feature flags

### lz4

Allows using `LZ4` compression, powered by [`lz4_flex`](https://github.com/PSeitz/lz4_flex).

*Disabled by default.*

### bytes

Uses [`bytes`](https://github.com/tokio-rs/bytes) as the underlying `Slice` type.

*Disabled by default.*

## Stable disk format

The disk format is stable as of 1.0.0.

2.0.0 uses a new disk format and needs a manual format migration.

Future breaking changes will result in a major version bump and a migration path.

## Run unit benchmarks

```bash
cargo bench --features lz4
```

## License

All source code is licensed under MIT OR Apache-2.0.

All contributions are to be licensed as MIT OR Apache-2.0.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://github.com/facebook/rocksdb/wiki/BlobDB

[3] https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
