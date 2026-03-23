<p align="center">
  <img src="/logo.png" height="160">
</p>

[![CI](https://github.com/structured-world/coordinode-lsm-tree/actions/workflows/coordinode-ci.yml/badge.svg)](https://github.com/structured-world/coordinode-lsm-tree/actions/workflows/coordinode-ci.yml)
[![Upstream CI](https://github.com/fjall-rs/lsm-tree/actions/workflows/test.yml/badge.svg)](https://github.com/fjall-rs/lsm-tree/actions/workflows/test.yml)
[![docs.rs](https://img.shields.io/docsrs/coordinode-lsm-tree?color=green)](https://docs.rs/coordinode-lsm-tree)
[![Crates.io](https://img.shields.io/crates/v/coordinode-lsm-tree?color=blue)](https://crates.io/crates/coordinode-lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.90.0-blue)

> **Maintained fork** by [Structured World Foundation](https://sw.foundation) for the [CoordiNode](https://github.com/structured-world/coordinode) database engine.
> Based on [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree). We contribute patches upstream and maintain additional features needed for CoordiNode (zstd compression, custom sequence number generators, batch get, intra-L0 compaction, security hardening).

> [!IMPORTANT]
> This fork now introduces a fork-specific **disk format V4** compatibility boundary.
> `V4` is a breaking on-disk change relative to `V3` because the fork persists new semantics such as range tombstones and merge operands.
> New code may continue reading supported `V3` databases, but databases written with these `V4` semantics must not be opened by older `V3` binaries.

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
- File descriptor caching with upper bound to reduce `fopen` syscalls
- *AMQ* filters (currently Bloom filters) to improve point lookup performance
- Multi-versioning of KVs, enabling snapshot reads
- Optionally partitioned block index & filters for better cache efficiency [[1]](#footnotes)
- Leveled and FIFO compaction
- Optional key-value separation for large value workloads [[2]](#footnotes), with automatic garbage collection
- Single deletion tombstones ("weak" deletion)
- Optional compaction filters to run custom logic during compactions

Keys are limited to 65536 bytes, values are limited to 2^32 bytes.
As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.

## Sponsors

<a href="https://sqlsync.dev">
  <picture>
    <source width="240" alt="Orbitinghail" media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/d22fcb1e6966ce08327ea3bf6cf2ea86a840b071/public/logos/orbitinghail.svg" />
    <source width="240" alt="Orbitinghail" media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/d22fcb1e6966ce08327ea3bf6cf2ea86a840b071/public/logos/orbitinghail_dark.svg" />
    <img width="240" alt="Orbitinghail" src="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/d22fcb1e6966ce08327ea3bf6cf2ea86a840b071/public/logos/orbitinghail_dark.svg" />
  </picture>
</a>

## Feature flags

### lz4

Allows using `LZ4` compression, powered by [`lz4_flex`](https://github.com/PSeitz/lz4_flex).

*Disabled by default.*

### bytes

Uses [`bytes`](https://github.com/tokio-rs/bytes) as the underlying `Slice` type.

*Disabled by default.*

## Run unit benchmarks

```bash
cargo bench --features lz4
```

## Support the Project

<div align="center">

![USDT TRC-20 Donation QR Code](assets/usdt-qr.svg)

USDT (TRC-20): `TFDsezHa1cBkoeZT5q2T49Wp66K8t2DmdA`

</div>

## License

All source code is licensed under MIT OR Apache-2.0.

All contributions are to be licensed as MIT OR Apache-2.0.

Original project by [fjall-rs](https://github.com/fjall-rs/lsm-tree). This fork is maintained by [Structured World Foundation](https://sw.foundation).

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://github.com/facebook/rocksdb/wiki/BlobDB

[3] https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
