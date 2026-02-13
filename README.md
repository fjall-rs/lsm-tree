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
- File descriptor caching with upper bound to reduce `fopen` syscalls
- *AMQ* filters (currently Bloom filters) to improve point lookup performance
- Multi-versioning of KVs, enabling snapshot reads
- Optionally partitioned block index & filters for better cache efficiency [[1]](#footnotes)
- Leveled and FIFO compaction
- Optional key-value separation for large value workloads [[2]](#footnotes), with automatic garbage collection
- Single deletion tombstones ("weak" deletion)

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

### tool

Enables the `lsm` CLI binary for interacting with LSM trees from the command line.

*Disabled by default.*

## CLI Tool

The crate includes an optional CLI tool (`lsm`) for inspecting and manipulating LSM trees.

### Installation

```bash
cargo install lsm-tree --features tool
```

Or build from source:

```bash
cargo build --release --features tool
```

### Usage

The tool can be used either with direct commands or in interactive shell mode.

#### Direct Commands

```bash
# Set a key-value pair
lsm /path/to/db set mykey "my value"

# Get a value
lsm /path/to/db get mykey

# Delete a key
lsm /path/to/db del mykey

# List all keys (aliases: list, ls)
lsm /path/to/db scan

# List keys with a prefix
lsm /path/to/db scan "user:"

# List keys in a range [start, end)
lsm /path/to/db range a z

# Count items
lsm /path/to/db count

# Show database info
lsm /path/to/db info

# Flush memtable to disk
lsm /path/to/db flush

# Run compaction
lsm /path/to/db compact
```

#### Interactive Shell

Start an interactive shell by running without a command:

```bash
lsm /path/to/db
```

The shell supports all the above commands plus:

- `begin` - Start a batch/transaction
- `commit` - Commit the current batch
- `rollback` - Discard the current batch
- `exit` / `quit` - Exit (flushes data first)
- `abort` - Exit without flushing
- `help` - Show available commands

#### Batch Operations

The shell supports batching multiple operations into an atomic unit:

```
lsm> begin
OK (batch started)
lsm> set key1 value1
OK (batched, ready to commit)
lsm> set key2 value2
OK (batched, ready to commit)
lsm> del key3
OK (batched, ready to commit)
lsm> commit
OK (batch committed, ready to flush)
```

While a batch is active:
- `get` reads from the batch first, then falls back to the tree
- `scan` and `range` warn that they ignore uncommitted batch operations
- `info` shows the pending batch operations
- `rollback` discards all batched operations

#### Long Scan

Use `-l` / `--long` to show internal entry details including sequence numbers, value types, and tombstones:

```
lsm> scan -l
=== Active Memtable ===
key1 = value1 [seqno=0, type=Value]
key2 [seqno=1, type=Tombstone]

=== Persisted (on disk) ===
key3 = value3 [seqno=2, type=Value]

(3 total items, 2 in memtable, 1 persisted, 1 tombstones)
```

#### Blob Trees with Indirect Items

A blob tree uses key-value separation, storing large values in separate blob files and keeping indirect references (indirections) in the main LSM-tree. This improves performance for large values by reducing write amplification and improving compaction efficiency.

To create a blob tree, use the `--blob-tree` flag along with `--separation-threshold` (or `-t`) to specify the size threshold in bytes. Values larger than this threshold will be stored as indirect items:

```bash
# Create a blob tree with 1 KiB separation threshold
lsm --blob-tree --separation-threshold 1024 /path/to/db set largekey "very large value..."

# Or using the short form
lsm -b -t 1KiB /path/to/db set largekey "very large value..."

# In interactive mode
lsm --blob-tree -t 1024 /path/to/db
lsm> set largekey "very large value..."
OK (set)
lsm> flush
OK (flushed)
lsm> scan -l
=== Active Memtable ===
=== Persisted (on disk) ===
largekey = very large value... [seqno=0, type=Indirection]
```

After flushing, values that exceed the separation threshold will appear as `type=Indirection` in verbose scan output, indicating they are stored in separate blob files rather than inline in the table.

#### Weak Tombstones

A weak tombstone is a special type of deletion marker that provides a "single deletion" semantic. Unlike regular tombstones, weak tombstones are designed to be removed during compaction when they encounter the key they mark for deletion, making them useful for scenarios where you want to delete a key but don't need the tombstone to persist indefinitely.

To delete a key with a weak tombstone, use the `--weak` (or `-w`) flag:

```bash
# Delete with weak tombstone from command line
lsm /path/to/db del --weak mykey

# Or using the short form
lsm /path/to/db del -w mykey

# In interactive shell
lsm> del --weak mykey
OK
lsm> scan -l
=== Active Memtable ===
mykey [seqno=0, type=WeakTombstone]
```

**Important notes:**
- Weak deletes are **not supported in batches** - they always execute immediately
- Weak tombstones appear as `type=WeakTombstone` in long scan output
- Weak tombstones are typically removed during compaction when they encounter the deleted key
- Use weak tombstones when you want single-deletion semantics rather than persistent deletion markers

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
