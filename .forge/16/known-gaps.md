# Issue #16: Range Tombstones — Known Gaps & Limitations

**Date:** 2026-03-16
**Status:** All critical bugs fixed. Remaining items are optimization opportunities.

## Fixed Bugs (this session)

1. **RT-only flush** — Writer deleted empty table before writing RTs. Fixed: derive key range from tombstones when item_count==0.
2. **MultiWriter rotation** — RTs only written to last output table. Fixed: clip RTs to each table's key range via `intersect_opt()` during rotation and finish.
3. **Compaction clipping** — RTs propagated unclipped. Fixed: MultiWriter now clips via `set_range_tombstones()`.

## Remaining Known Limitations

### 1. No WAL persistence for range tombstones
Range tombstones in the active memtable are lost on crash (before flush). This is consistent with the crate's design — it does not ship a WAL. The caller (fjall) manages WAL-level durability.

### 2. No compaction-level tombstone clipping for multi-run tables
When a run has multiple tables, the `RunReader` path (in `range.rs`) does not apply the table-skip optimization. Only single-table runs get the `is_covered` check. This is a performance optimization gap, not a correctness issue — the `RangeTombstoneFilter` still correctly filters suppressed items.

### 3. Linear scan for SST range tombstone suppression in point reads
`is_suppressed_by_range_tombstones` iterates ALL SST tables and ALL their range tombstones linearly. For workloads with many tables and many range tombstones, this could degrade point read latency. Consider: building an in-memory interval tree from SST tombstones on version change (similar to GLORAN paper's approach).

### 4. Range tombstone block format is not upstream-compatible
We use a raw wire format (`[start_len:u16][start][end_len:u16][end][seqno:u64]`) with `BlockType::RangeTombstone = 4`. This is a fork-only format. When upstream settles on a format, we'll need a migration.

### 5. No range tombstone metrics
Unlike point tombstones (`tombstone_count`, `weak_tombstone_count`), range tombstones are tracked in table metadata (`range_tombstone_count`) but not surfaced to the `Metrics` system (behind `#[cfg(feature = "metrics")]`).
