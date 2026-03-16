# Issue #16: Range Tombstones / delete_range / delete_prefix

## Deep Architecture Analysis

**Date:** 2026-03-16
**Branch:** `feat/#16-feat-range-tombstones--deleterange--deleteprefix`
**Upstream reference:** fjall-rs/lsm-tree#2 (epic), fjall-rs/lsm-tree#242 (PR, OPEN but stalled)

---

## Context

### What exists today

The codebase has **point tombstones** (`ValueType::Tombstone`, `ValueType::WeakTombstone`) and a limited `drop_range` compaction strategy that drops SST files **fully contained** within a key range. There is no true range tombstone — a single marker that logically deletes all keys in `[start, end)`.

**Current deletion model:**
- `remove(key, seqno)` → writes a `Tombstone` entry to memtable
- `remove_weak(key, seqno)` → writes a `WeakTombstone` (single-delete)
- `drop_range(range)` → compaction strategy that physically drops fully-contained tables (no MVCC, no partial overlap handling)
- No `delete_range` or `delete_prefix` API

### Why CoordiNode needs this

Graph operations like "drop all edges of node X" or "drop label Y" generate O(N) individual tombstones for N edges/properties. With range tombstones:
- **Single marker** replaces millions of point tombstones
- **Compaction efficiency** — one range tombstone suppresses entire key ranges without per-key processing
- **Label drop** becomes O(1) write instead of O(N)

---

## Upstream PR #242 — Deep Analysis

### Status: STALLED with unresolved architectural disagreement

**Timeline:**
- 2026-02-06: PR opened by `temporaryfix` (4049 additions, 15 commits)
- 2026-02-06: Author: "Will do benchmarks when I get the chance" — **benchmarks never done**
- 2026-02-10: Maintainer `marvin-j97` reviews — raises **principal concern about SST block format**
- 2026-02-10: Extended discussion about block format design
- 2026-02-12: Author shares GLORAN paper, suggesting the whole approach may be wrong
- 2026-02-12: Maintainer is "open to alternative implementations" (GLORAN-style)
- 2026-03-15: Author: "life has gotten in the way" — **work stalled**

### Maintainer's principal concern

**marvin-j97 objects to the dual block format** (`RangeTombstoneStart` + `RangeTombstoneEnd`):

> "The first thing that seems awkward to me is the duplication of range tombstones for start and end inside tables. Ideally you would just store a table's range tombstones using a DataBlock."

His reasoning:
1. DataBlocks are "tried and tested" — adding new block types is maintenance burden
2. RocksDB stores range tombstones "in data blocks without binary seek index" (cites blog post)
3. The 2D problem (range + seqno) is "pretty unavoidable" — range tombstones need in-memory deserialization on table open regardless (like RocksDB's fragmentation)
4. Suggests looking at **Pebble** for alternative approach

Author's defense:
- DataBlocks are optimized for key→value lookups, not overlap/cover checks
- Reverse MVCC needs streaming on `(end > current_key)` — impossible with only start-sorted data
- ByStart + ByEndDesc enables per-window `max_end` pruning and streaming reverse activation
- RocksDB also uses dedicated blocks, not regular data blocks

**Unresolved:** Maintainer did not accept the defense. The conversation shifted to GLORAN paper, and then stalled.

### GLORAN paper (arxiv 2511.06061)

Both maintainer and author reference this paper as potentially superior:
- **Problem:** Per-level range tombstones cause 30% point lookup degradation with just 1% range deletes
- **Solution:** Global Range Record Index (LSM-DRtree) — central immutable index instead of per-level storage
- **Results:** 10.6× faster point lookups, 2.7× higher throughput
- **Maintainer's take:** "Such auxiliary indexes must be immutable/log-structured" — possible to add as new field in Version files (like value log)

### What this means for us

1. **PR #242 will NOT merge as-is** — maintainer wants different SST format
2. **No timeline for resolution** — author is busy, maintainer is open to alternatives but hasn't specified one
3. **Cherry-picking is high-risk** — we'd inherit a contested design that upstream may reject/replace
4. **The PR base is stale** — based on pre-3.1.0 upstream; upstream has diverged significantly since

---

## Fork Divergence Analysis

### Our fork vs upstream main
```
origin/main vs upstream/main:
  45 files changed, 224 insertions(+), 3,789 deletions(-)
```
Our fork has ~100+ commits with: zstd compression, intra-L0 compaction, size cap enforcement, verify_integrity, SequenceNumberGenerator trait, multi_get, contains_prefix, seqno-aware seek, copilot CI, release-plz.

### Our fork vs PR #242 branch
```
origin/main vs upstream-pr-242:
  97 files changed, 4,568 insertions(+), 6,096 deletions(-)
```
Massive diff because:
1. PR #242 is based on OLD upstream (pre-3.1.0)
2. Our fork has features upstream doesn't have
3. Upstream main has moved past the PR base

**New files in PR #242:**
- `src/active_tombstone_set.rs` (403 lines)
- `src/memtable/interval_tree.rs` (513 lines)
- `src/range_tombstone.rs` (343 lines)
- `src/range_tombstone_filter.rs` (281 lines)
- `src/table/range_tombstone_block_by_end.rs` (393 lines)
- `src/table/range_tombstone_block_by_start.rs` (664 lines)
- `src/table/range_tombstone_encoder.rs` (365 lines)
- `tests/range_tombstone.rs` (447 lines)

**Heavily modified files (conflict-prone):**
- `src/abstract_tree.rs` (renamed to `abstract.rs` in PR!)
- `src/tree/mod.rs` — both we and PR modify heavily
- `src/compaction/stream.rs` — PR strips our `StreamFilter`
- `src/compaction/worker.rs` — both modify
- `src/memtable/mod.rs` — both modify
- `src/table/mod.rs` — both modify
- `src/blob_tree/mod.rs` — both modify
- `src/config/mod.rs` — PR removes features we added
- `src/compression.rs` — PR removes our zstd support
- `src/seqno.rs` — PR removes our SequenceNumberGenerator

**Critical:** The PR diff REMOVES many of our fork features because it's based on older upstream. Cherry-picking individual commits would require resolving conflicts in virtually every modified file.

---

## Revised Implementation Approaches

### Approach A: Selective port of PR #242 core logic — **RECOMMENDED**

Extract the **algorithmic core** from PR #242 (types, interval tree, active set, filter) and integrate into our fork manually. Skip the contested SST block format — use DataBlock-based storage as the maintainer prefers. This positions us for eventual upstream convergence regardless of which SST format upstream chooses.

**What to port (new files, minimal conflicts):**
1. `RangeTombstone` struct + `ActiveTombstoneSet` sweep-line tracker (~750 LOC)
2. `IntervalTree` for memtable range tombstones (~500 LOC)
3. `RangeTombstoneFilter` for range/prefix iteration (~280 LOC)

**What to write ourselves:**
4. SST persistence — use existing `DataBlock` with `key=start, value=end|seqno` (follows maintainer's direction)
5. Integration into our fork's `tree/mod.rs`, `abstract_tree.rs`, `compaction/` (our code differs too much to cherry-pick)
6. `delete_range` / `delete_prefix` API on `AbstractTree`

**Pros:**
- Reuses proven algorithms (interval tree, sweep-line) without inheriting contested SST format
- Follows maintainer's preferred direction (DataBlock reuse) — better upstream merge path
- New files (types, interval tree, active set) can be ported with zero conflicts
- Integration code written against OUR fork, not upstream's old base
- Benchmarks can be done before committing to SST format

**Cons:**
- More manual work than pure cherry-pick (~3-4 days vs 2-3)
- SST format may still diverge from whatever upstream eventually picks
- Need to validate the ported algorithms ourselves

**Estimate:** 3-4 days

**Upstream-compatible:** Partially — core algorithms match; SST format aligned with maintainer preference but final upstream choice unknown.

---

### Approach B: Memtable-only + DataBlock persistence (minimal viable)

Implement range tombstones in memtable with a simple sorted structure. Persist to SSTs using existing DataBlock (no new block types). Skip sweep-line optimization — use simpler linear scan for iteration filtering. Good enough for CoordiNode's initial needs, easy to upgrade later.

**How it works:**
1. `RangeTombstone` struct with `[start, end)` + seqno
2. `BTreeMap<(start, Reverse(seqno)), RangeTombstone>` in Memtable (simpler than full interval tree)
3. Point reads: linear scan memtable tombstones for suppression (O(T) where T = tombstone count, typically small)
4. Range iteration: collect tombstones, suppress matching keys inline
5. SST: store tombstones in DataBlock at end of table (key=start, value=end|seqno)
6. Compaction: read tombstone DataBlock, clip to table range, write to output
7. GC: evict tombstones below watermark at bottom level

**Pros:**
- Simpler implementation (~1.5-2K LOC)
- No new block types — uses proven DataBlock format
- Good enough for CoordiNode's workload (tens of tombstones, not millions)
- Easy to upgrade to interval tree / sweep-line later if needed
- Aligns with maintainer's DataBlock preference

**Cons:**
- Linear scan for suppression = O(T) per point read (fine for small T, bad for thousands of tombstones)
- No sweep-line = reverse iteration is naive
- Need to write everything from scratch (no code reuse from PR)
- No table-skip optimization initially

**Estimate:** 2-3 days

**Upstream-compatible:** Partially — SST format aligned with maintainer, but algorithms differ.

---

### Approach C: Wait for upstream resolution

Don't implement now. Monitor PR #242 discussion. Use `drop_range` + point tombstones as workaround.

**Pros:**
- Zero fork divergence
- Zero effort now

**Cons:**
- Blocks CoordiNode's efficient bulk deletion (P0 requirement)
- Upstream resolution could take months (author stalled, no consensus on design)
- `drop_range` is not MVCC-safe — unusable for transactional graph operations

**Estimate:** 0 days now, unknown later

**Upstream-compatible:** Perfect — but at the cost of blocking our product.

---

### Approach D: Full custom (GLORAN-inspired)

Implement range tombstones using GLORAN's global index approach. Independent of upstream.

**Pros:**
- Potentially best performance (10x point lookups per paper)
- Novel approach both maintainer and PR author seem interested in

**Cons:**
- Research-grade, no production implementation exists
- Massive effort (5-8 days) with high uncertainty
- Permanent fork divergence
- Paper is from 2025, may have undiscovered issues

**Estimate:** 5-8 days

**Upstream-compatible:** No.

---

## Comparison Matrix

| Criteria | A: Selective Port | B: Minimal Viable | C: Wait | D: GLORAN |
|---|---|---|---|---|
| **Completeness** | Full | Adequate | None | Full |
| **MVCC correctness** | Yes | Yes | No | Yes |
| **SST persistence** | DataBlock | DataBlock | No | Custom |
| **Point read overhead** | O(log T) | O(T) | None | O(log² N) |
| **Sweep-line iteration** | Yes | No | N/A | Yes |
| **Table skip optimization** | Yes | No | No | Yes |
| **Upstream alignment** | Good | Moderate | Perfect | None |
| **Implementation effort** | 3-4d | 2-3d | 0d | 5-8d |
| **Fork divergence risk** | Low-Medium | Low | None | Very High |
| **CoordiNode P0 unblock** | Yes | Yes | No | Yes |

---

## Recommendation: Approach A — Selective Port of PR #242 Core

**Why A over the original "cherry-pick everything":**

1. **PR #242 will not merge as-is** — maintainer rejected the SST format. Cherry-picking inherits a dead design
2. **PR base is stale** — based on pre-3.1.0 upstream, conflict resolution across 25+ files is a week of work, not 2-3 days
3. **Core algorithms are solid** — `IntervalTree`, `ActiveTombstoneSet`, `RangeTombstoneFilter` are well-designed and can be ported as standalone files with zero conflicts
4. **DataBlock SST format** follows maintainer's direction — better upstream merge path than the dual-block approach
5. **We control integration** — writing our own glue code against our fork's actual state avoids the massive conflict resolution

**Execution plan:**
1. Port `RangeTombstone` + `ActiveTombstoneSet` types (new files, no conflicts)
2. Port `IntervalTree` for memtable (new file, no conflicts)
3. Port `RangeTombstoneFilter` for iteration (new file, no conflicts)
4. Design DataBlock-based SST persistence (new block type value = `5`, or reuse DataBlock with sentinel key prefix)
5. Integrate into memtable, point reads, range iteration, flush, compaction
6. Add `remove_range` / `remove_prefix` to `AbstractTree`
7. Write tests (port + adapt from PR #242's test suite)
8. Benchmark point read regression

**Key risks:**
- The ported algorithms may have bugs we don't catch (mitigate: thorough testing)
- Upstream may pick a different SST format than DataBlock (mitigate: encapsulate persistence behind trait/module boundary)
- Performance regression on point reads (mitigate: fast path when zero range tombstones exist, as PR #242 already does)
