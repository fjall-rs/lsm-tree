#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry, PinningPolicy};
use lsm_tree::prefix::{FixedPrefixExtractor, PrefixExtractor, SharedPrefixExtractor};
use lsm_tree::{AbstractTree, AnyTree, Guard, IterGuardImpl, SequenceNumberCounter};
use std::ops::Bound;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Extractors: each one follows the contiguity rule, and covers a different
// way the read path can find a sound probe
// ---------------------------------------------------------------------------

/// Emits a 2-byte and a 4-byte prefix, so keys have several prefixes and the
/// writer dedups each one on its own.
struct Hierarchical;

impl PrefixExtractor for Hierarchical {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        if key.len() >= 2 {
            emit(2);
        }
        if key.len() >= 4 {
            emit(4);
        }
    }

    fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
        matches!(prefix.len(), 2 | 4)
    }

    fn name(&self) -> &str {
        "fuzz::hierarchical"
    }
}

/// Emits every prefix that ends in the delimiter byte 3, like path segments.
struct Segments;

impl PrefixExtractor for Segments {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        for (i, &byte) in key.iter().enumerate() {
            if byte == 3 {
                emit(i + 1);
            }
        }
    }

    fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
        prefix.last() == Some(&3)
    }

    fn name(&self) -> &str {
        "fuzz::segments"
    }
}

/// Emits 2 bytes when the third byte is below 0x80 (or missing), else 3, so
/// keys starting with one 2-byte string can have different prefixes. Without
/// the appended hook only ranges whose bounds share a prefix are probed.
struct SplitWidth;

impl PrefixExtractor for SplitWidth {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        match key.get(2) {
            _ if key.len() < 2 => {}
            Some(&byte) if byte >= 0x80 => emit(3),
            _ => emit(2),
        }
    }

    fn name(&self) -> &str {
        "fuzz::split_width"
    }
}

/// A 2-byte prefix under a name no other extractor shares, so after a switch
/// to another extractor its tables have no usable prefix filter.
struct Opaque;

impl PrefixExtractor for Opaque {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        if key.len() >= 2 {
            emit(2);
        }
    }

    fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
        prefix.len() == 2
    }

    fn name(&self) -> &str {
        "fuzz::opaque"
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum ExtractorChoice {
    FixedPrefix1,
    FixedPrefix2,
    FixedPrefix3,
    FixedPrefix4,
    Hierarchical,
    Segments,
    SplitWidth,
    Opaque,
}

impl ExtractorChoice {
    fn into_extractor(&self) -> SharedPrefixExtractor {
        match self {
            Self::FixedPrefix1 => Arc::new(FixedPrefixExtractor::new(1)),
            Self::FixedPrefix2 => Arc::new(FixedPrefixExtractor::new(2)),
            Self::FixedPrefix3 => Arc::new(FixedPrefixExtractor::new(3)),
            Self::FixedPrefix4 => Arc::new(FixedPrefixExtractor::new(4)),
            Self::Hierarchical => Arc::new(Hierarchical),
            Self::Segments => Arc::new(Segments),
            Self::SplitWidth => Arc::new(SplitWidth),
            Self::Opaque => Arc::new(Opaque),
        }
    }
}

/// Size of each memtable's prefix bloom filter
#[derive(Arbitrary, Debug, Clone)]
enum MemtableBloomChoice {
    Off,
    /// One cache line, so most probes are false positives
    Tiny,
    Sized,
}

impl MemtableBloomChoice {
    fn bytes(&self) -> usize {
        match self {
            Self::Off => 0,
            Self::Tiny => 1,
            Self::Sized => 4_096,
        }
    }
}

/// What the tree with the extractor is opened with
#[derive(Arbitrary, Debug, Clone)]
struct FilterSetup {
    extractor: ExtractorChoice,
    whole_key_filtering: bool,
    memtable_bloom: MemtableBloomChoice,
}

#[derive(Arbitrary, Debug, Clone)]
enum BpkChoice {
    Low,
    Default,
    High,
}

impl BpkChoice {
    fn value(&self) -> f32 {
        match self {
            Self::Low => 1.0,
            Self::Default => 10.0,
            Self::High => 50.0,
        }
    }
}

/// Whether filter blocks are partitioned (two-level index) or full (single block).
/// The bug we found (empty tli_handles panic) was specifically in the partitioned
/// writer, so we want AFL to control this directly.
#[derive(Arbitrary, Debug, Clone)]
enum FilterPartitioningChoice {
    /// Default policy: full on L0-L2, partitioned on L3+.
    Default,
    /// Partitioned on ALL levels — forces partitioned writer even at flush time.
    AllPartitioned,
    /// Never partitioned — full filter on all levels.
    NeverPartitioned,
}

// ---------------------------------------------------------------------------
// Clustered key/prefix types — small alphabet, bounded length
// ---------------------------------------------------------------------------

/// Bytes that change how keys split into prefixes: 0 (a successor), 3 (the
/// `Segments` delimiter), 0x7F/0x80 (the `SplitWidth` split) and 0xFE/0xFF
/// (runs no successor can end)
const BYTES: [u8; 8] = [0, 1, 3, 0x7F, 0x80, 0x81, 0xFE, 0xFF];

/// A key of 1..=6 bytes: the first from 0..4, the rest from [`BYTES`], so keys
/// often share prefixes of every length the extractors emit.
#[derive(Debug, Clone)]
struct ClusteredKey(Vec<u8>);

impl<'a> Arbitrary<'a> for ClusteredKey {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len: usize = u.int_in_range(1..=6)?;
        let mut key = Vec::with_capacity(len);
        key.push(u.int_in_range(0..=3)?);
        for _ in 1..len {
            key.push(*u.choose(&BYTES)?);
        }
        Ok(ClusteredKey(key))
    }
}

/// A prefix of 0..=5 bytes, shorter than, equal to and longer than the
/// extractors' prefixes. The first byte is from 0..5, so 4 starts prefixes
/// no key has; the rest are from [`BYTES`] or 0x42, which no key has.
#[derive(Debug, Clone)]
struct ClusteredPrefix(Vec<u8>);

impl<'a> Arbitrary<'a> for ClusteredPrefix {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len: usize = u.int_in_range(0..=5)?;
        let mut prefix = Vec::with_capacity(len);
        if len > 0 {
            prefix.push(u.int_in_range(0..=4)?);
        }
        for _ in 1..len {
            prefix.push(if u.ratio(1, 10)? {
                0x42
            } else {
                *u.choose(&BYTES)?
            });
        }
        Ok(ClusteredPrefix(prefix))
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum BoundChoice {
    Included(ClusteredKey),
    Excluded(ClusteredKey),
    Unbounded,
}

impl BoundChoice {
    fn to_bound(&self) -> Bound<Vec<u8>> {
        match self {
            Self::Included(key) => Bound::Included(key.0.clone()),
            Self::Excluded(key) => Bound::Excluded(key.0.clone()),
            Self::Unbounded => Bound::Unbounded,
        }
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

#[derive(Arbitrary, Debug, Clone)]
enum Op {
    // --- Writes ---
    Insert {
        key: ClusteredKey,
        value_len: u8,
        value_seed: u8,
    },
    Delete {
        key: ClusteredKey,
    },
    /// Weak tombstone: marks a key as weakly deleted. During compaction GC,
    /// a weak tombstone paired with a value below the GC watermark causes the
    /// weak tombstone to be dropped. Tests that prefix filter correctness is
    /// preserved after weak tombstone GC.
    WeakDelete {
        key: ClusteredKey,
    },

    // --- Structure ops ---
    /// Seals the active memtable without flushing it, so reads merge
    /// several memtables
    Rotate,
    Flush,
    Compact,
    MajorCompact {
        /// Target table size for major compaction. Small values produce
        /// runs of many tables, where a range either probes the one table it
        /// overlaps or spans several and probes none.
        small_tables: bool,
    },
    /// Clean close + reopen with the same extractor.
    Reopen,
    /// Close + reopen with another extractor and `whole_key_filtering`: old
    /// tables keep reading with the extractor that wrote them, new flushes
    /// use the new one.
    ReopenNewExtractor {
        setup: FilterSetup,
    },

    // --- Point reads ---
    Get {
        key: ClusteredKey,
    },
    ContainsKey {
        key: ClusteredKey,
    },

    // --- Scans ---
    PrefixScan {
        prefix: ClusteredPrefix,
    },
    PrefixScanRev {
        prefix: ClusteredPrefix,
    },
    /// Bidirectional iterator stepping on a prefix scan.
    /// Each bool in `directions` controls: true = next_back, false = next.
    PrefixPingPong {
        prefix: ClusteredPrefix,
        directions: Vec<bool>,
    },
    RangeScan {
        start: ClusteredKey,
        end: ClusteredKey,
    },
    RangeScanRev {
        start: ClusteredKey,
        end: ClusteredKey,
    },
    /// Range with any mix of included, excluded and unbounded ends, as each
    /// kind takes its own path to the probe prefix.
    RangeScanBounds {
        start: BoundChoice,
        end: BoundChoice,
        reverse: bool,
    },
    /// Unbounded iteration endpoints. Tests first_key_value / last_key_value
    /// which use `range(..)` internally — prefix filters should not interfere
    /// with unbounded scans.
    FirstKV,
    LastKV,

    /// Prefix scan using the first N bytes of a previously inserted key.
    /// This guarantees the prefix overlaps real data, forcing the filter
    /// to make a meaningful decision rather than trivially matching nothing.
    PrefixScanExistingKey {
        prefix_len: u8,
    },

    /// Flush and compact into small tables, then reopen with another
    /// extractor, so runs mix tables written by different extractors.
    FlushCompactReopenCompact {
        setup: FilterSetup,
    },

    // --- MVCC ---
    /// Capture the current visibility seqno as a snapshot. Subsequent
    /// SnapshotGet / SnapshotPrefixScan ops read at this frozen point
    /// while newer writes continue advancing the seqno.
    TakeSnapshot,
    /// Point read at the most recently taken snapshot seqno.
    SnapshotGet {
        key: ClusteredKey,
    },
    /// Prefix scan at the most recently taken snapshot seqno.
    SnapshotPrefixScan {
        prefix: ClusteredPrefix,
    },
}

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    setup: FilterSetup,
    bpk: BpkChoice,
    filter_partitioning: FilterPartitioningChoice,
    ops: Vec<Op>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn collect_kv(iter: impl Iterator<Item = IterGuardImpl>) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|g| {
        let (k, v) = g.into_inner().expect("iterator item should be readable");
        (k.to_vec(), v.to_vec())
    })
    .collect()
}

fn open_tree(
    dir: &tempfile::TempDir,
    seqno: &SequenceNumberCounter,
    vis: &SequenceNumberCounter,
    setup: Option<&FilterSetup>,
    bloom_bpk: f32,
    filter_partitioning: &FilterPartitioningChoice,
) -> AnyTree {
    let mut config =
        lsm_tree::Config::new(dir, seqno.clone(), vis.clone()).filter_policy(FilterPolicy::all(
            FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(bloom_bpk)),
        ));
    if let Some(setup) = setup {
        config = config
            .prefix_extractor(setup.extractor.into_extractor())
            .whole_key_filtering(setup.whole_key_filtering)
            .memtable_prefix_bloom_bytes(setup.memtable_bloom.bytes());
    }
    config = match filter_partitioning {
        FilterPartitioningChoice::Default => config,
        FilterPartitioningChoice::AllPartitioned => {
            config.filter_block_partitioning_policy(PinningPolicy::all(true))
        }
        FilterPartitioningChoice::NeverPartitioned => {
            config.filter_block_partitioning_policy(PinningPolicy::all(false))
        }
    };
    config.open().unwrap()
}

fn ordered_range<'a>(a: &'a [u8], b: &'a [u8]) -> (&'a [u8], &'a [u8]) {
    if a <= b { (a, b) } else { (b, a) }
}

fn make_value(len: u8, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i)).collect()
}

// ---------------------------------------------------------------------------
// Oracle test: tree with prefix extractor vs tree without
//
// Both trees receive identical operations. Any read that returns different
// results means the prefix filter wrongly excluded data = silent data loss.
// ---------------------------------------------------------------------------

fn run_oracle_test(
    initial_setup: &FilterSetup,
    bloom_bpk: f32,
    filter_partitioning: &FilterPartitioningChoice,
    ops: &[Op],
) {
    let dir_with = tempfile::tempdir().unwrap();
    let dir_without = tempfile::tempdir().unwrap();

    let seqno_with = SequenceNumberCounter::default();
    let seqno_without = SequenceNumberCounter::default();
    let vis_with = SequenceNumberCounter::default();
    let vis_without = SequenceNumberCounter::default();

    // Current setup of tree_with (may change via ReopenNewExtractor)
    let mut current_setup = initial_setup.clone();

    let mut tree_with = open_tree(
        &dir_with,
        &seqno_with,
        &vis_with,
        Some(&current_setup),
        bloom_bpk,
        filter_partitioning,
    );
    let mut tree_without = open_tree(
        &dir_without,
        &seqno_without,
        &vis_without,
        None,
        bloom_bpk,
        filter_partitioning,
    );

    let compaction_strategy = Arc::new(lsm_tree::compaction::Leveled::default());

    // MVCC snapshot seqnos. `None` until TakeSnapshot is hit.
    let mut snapshot_seqno_with: Option<u64> = None;
    let mut snapshot_seqno_without: Option<u64> = None;

    // Track inserted keys so PrefixScanExistingKey can derive prefixes
    // from real data.
    let mut inserted_keys: Vec<Vec<u8>> = Vec::new();

    for (i, op) in ops.iter().enumerate() {
        match op {
            // ----- Writes -----
            Op::Insert {
                key,
                value_len,
                value_seed,
            } => {
                let key = &key.0;
                let value = make_value(*value_len, *value_seed);
                let s1 = seqno_with.next();
                let s2 = seqno_without.next();
                tree_with.insert(key.clone(), value.clone(), s1);
                tree_without.insert(key.clone(), value.clone(), s2);
                vis_with.fetch_max(s1 + 1);
                vis_without.fetch_max(s2 + 1);
                if inserted_keys.len() < 100 {
                    inserted_keys.push(key.clone());
                }
            }

            Op::Delete { key } => {
                let key = &key.0;
                let s1 = seqno_with.next();
                let s2 = seqno_without.next();
                tree_with.remove(key.as_slice(), s1);
                tree_without.remove(key.as_slice(), s2);
                vis_with.fetch_max(s1 + 1);
                vis_without.fetch_max(s2 + 1);
            }

            Op::WeakDelete { key } => {
                let key = &key.0;
                let s1 = seqno_with.next();
                let s2 = seqno_without.next();
                tree_with.remove_weak(key.clone(), s1);
                tree_without.remove_weak(key.clone(), s2);
                vis_with.fetch_max(s1 + 1);
                vis_without.fetch_max(s2 + 1);
            }

            // ----- Structure ops -----
            Op::Rotate => {
                tree_with.rotate_memtable();
                tree_without.rotate_memtable();
            }

            Op::Flush => {
                tree_with.flush_active_memtable(0).unwrap();
                tree_without.flush_active_memtable(0).unwrap();
            }

            Op::Compact => {
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let _ = tree_with.compact(compaction_strategy.clone(), s1);
                let _ = tree_without.compact(compaction_strategy.clone(), s2);
                // Compaction may GC old super versions, invalidating the
                // raw snapshot seqno we captured (we don't pin it via the
                // tree's snapshot API, so the version can be purged).
                snapshot_seqno_with = None;
                snapshot_seqno_without = None;
            }

            Op::MajorCompact { small_tables } => {
                let target = if *small_tables { 128 } else { 4_096 };
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let _ = tree_with.major_compact(target, s1);
                let _ = tree_without.major_compact(target, s2);
                snapshot_seqno_with = None;
                snapshot_seqno_without = None;
            }

            Op::Reopen => {
                drop(tree_with);
                drop(tree_without);
                tree_with = open_tree(
                    &dir_with,
                    &seqno_with,
                    &vis_with,
                    Some(&current_setup),
                    bloom_bpk,
                    filter_partitioning,
                );
                tree_without = open_tree(
                    &dir_without,
                    &seqno_without,
                    &vis_without,
                    None,
                    bloom_bpk,
                    filter_partitioning,
                );
                // Reopen rebuilds the super version list from scratch;
                // old snapshot seqnos are no longer valid.
                snapshot_seqno_with = None;
                snapshot_seqno_without = None;
            }

            Op::ReopenNewExtractor { setup } => {
                current_setup = setup.clone();
                drop(tree_with);
                drop(tree_without);
                tree_with = open_tree(
                    &dir_with,
                    &seqno_with,
                    &vis_with,
                    Some(&current_setup),
                    bloom_bpk,
                    filter_partitioning,
                );
                tree_without = open_tree(
                    &dir_without,
                    &seqno_without,
                    &vis_without,
                    None,
                    bloom_bpk,
                    filter_partitioning,
                );
                snapshot_seqno_with = None;
                snapshot_seqno_without = None;
            }

            // ----- Point reads -----
            Op::Get { key } => {
                let key = &key.0;
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let r1 = tree_with.get(key.as_slice(), s1).unwrap();
                let r2 = tree_without.get(key.as_slice(), s2).unwrap();
                assert_eq!(r1, r2, "op {i}: point read mismatch for key {key:?}");
            }

            Op::ContainsKey { key } => {
                let key = &key.0;
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let r1 = tree_with.contains_key(key.as_slice(), s1).unwrap();
                let r2 = tree_without.contains_key(key.as_slice(), s2).unwrap();
                assert_eq!(r1, r2, "op {i}: contains_key mismatch for key {key:?}");
            }

            // ----- Scans -----
            Op::PrefixScan { prefix } => {
                let prefix = &prefix.0;
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let a = collect_kv(tree_with.prefix(prefix.clone(), s1, None));
                let b = collect_kv(tree_without.prefix(prefix.clone(), s2, None));
                assert_eq!(a, b, "op {i}: prefix scan mismatch for {prefix:?}");
            }

            Op::PrefixScanRev { prefix } => {
                let prefix = &prefix.0;
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let a = collect_kv(tree_with.prefix(prefix.clone(), s1, None).rev());
                let b = collect_kv(tree_without.prefix(prefix.clone(), s2, None).rev());
                assert_eq!(a, b, "op {i}: reverse prefix scan mismatch for {prefix:?}");
            }

            Op::PrefixPingPong { prefix, directions } => {
                let prefix = &prefix.0;
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let mut iter_with = tree_with.prefix(prefix.clone(), s1, None);
                let mut iter_without = tree_without.prefix(prefix.clone(), s2, None);

                for (j, &go_back) in directions.iter().enumerate() {
                    let item_with = if go_back {
                        iter_with.next_back()
                    } else {
                        iter_with.next()
                    };
                    let item_without = if go_back {
                        iter_without.next_back()
                    } else {
                        iter_without.next()
                    };

                    let kv_with =
                        item_with.map(|g| g.into_inner().expect("iter item should be readable"));
                    let kv_without =
                        item_without.map(|g| g.into_inner().expect("iter item should be readable"));

                    match (&kv_with, &kv_without) {
                        (Some((k1, v1)), Some((k2, v2))) => {
                            assert_eq!(
                                (k1.as_ref(), v1.as_ref()),
                                (k2.as_ref(), v2.as_ref()),
                                "op {i} step {j}: ping-pong mismatch for prefix {prefix:?}, go_back={go_back}",
                            );
                        }
                        (None, None) => {}
                        _ => {
                            panic!(
                                "op {i} step {j}: ping-pong length mismatch for prefix {prefix:?}: \
                                 with={kv_with:?}, without={kv_without:?}",
                            );
                        }
                    }
                }
            }

            Op::RangeScan { start, end } => {
                let (lo, hi) = ordered_range(&start.0, &end.0);
                if lo == hi {
                    continue;
                }
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let a = collect_kv(tree_with.range(lo..hi, s1, None));
                let b = collect_kv(tree_without.range(lo..hi, s2, None));
                assert_eq!(a, b, "op {i}: range scan mismatch for {lo:?}..{hi:?}");
            }

            Op::RangeScanRev { start, end } => {
                let (lo, hi) = ordered_range(&start.0, &end.0);
                if lo == hi {
                    continue;
                }
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let a = collect_kv(tree_with.range(lo..hi, s1, None).rev());
                let b = collect_kv(tree_without.range(lo..hi, s2, None).rev());
                assert_eq!(
                    a, b,
                    "op {i}: reverse range scan mismatch for {lo:?}..{hi:?}"
                );
            }

            Op::RangeScanBounds {
                start,
                end,
                reverse,
            } => {
                let (start, end) = (start.to_bound(), end.to_bound());

                // Skip ranges that end before they start
                if let (
                    Bound::Included(lo) | Bound::Excluded(lo),
                    Bound::Included(hi) | Bound::Excluded(hi),
                ) = (&start, &end)
                {
                    let empty_point = lo == hi
                        && !(matches!(start, Bound::Included(_))
                            && matches!(end, Bound::Included(_)));
                    if lo > hi || empty_point {
                        continue;
                    }
                }

                let range = (start.clone(), end.clone());
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let (a, b) = if *reverse {
                    (
                        collect_kv(tree_with.range(range.clone(), s1, None).rev()),
                        collect_kv(tree_without.range(range, s2, None).rev()),
                    )
                } else {
                    (
                        collect_kv(tree_with.range(range.clone(), s1, None)),
                        collect_kv(tree_without.range(range, s2, None)),
                    )
                };
                assert_eq!(a, b, "op {i}: range scan mismatch for {start:?}..{end:?}");
            }

            Op::FirstKV => {
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let r1 = tree_with.first_key_value(s1, None).map(|g| {
                    g.into_inner()
                        .expect("first_key_value item should be readable")
                });
                let r2 = tree_without.first_key_value(s2, None).map(|g| {
                    g.into_inner()
                        .expect("first_key_value item should be readable")
                });
                match (&r1, &r2) {
                    (Some((k1, v1)), Some((k2, v2))) => {
                        assert_eq!(
                            (k1.as_ref(), v1.as_ref()),
                            (k2.as_ref(), v2.as_ref()),
                            "op {i}: first_key_value mismatch"
                        );
                    }
                    (None, None) => {}
                    _ => panic!(
                        "op {i}: first_key_value presence mismatch: with={r1:?}, without={r2:?}"
                    ),
                }
            }

            Op::LastKV => {
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let r1 = tree_with.last_key_value(s1, None).map(|g| {
                    g.into_inner()
                        .expect("last_key_value item should be readable")
                });
                let r2 = tree_without.last_key_value(s2, None).map(|g| {
                    g.into_inner()
                        .expect("last_key_value item should be readable")
                });
                match (&r1, &r2) {
                    (Some((k1, v1)), Some((k2, v2))) => {
                        assert_eq!(
                            (k1.as_ref(), v1.as_ref()),
                            (k2.as_ref(), v2.as_ref()),
                            "op {i}: last_key_value mismatch"
                        );
                    }
                    (None, None) => {}
                    _ => panic!(
                        "op {i}: last_key_value presence mismatch: with={r1:?}, without={r2:?}"
                    ),
                }
            }

            Op::PrefixScanExistingKey { prefix_len } => {
                if inserted_keys.is_empty() {
                    continue;
                }
                // Pick a key deterministically from the inserted set
                let key = &inserted_keys[i % inserted_keys.len()];
                let plen = (*prefix_len as usize % 6).min(key.len());
                let prefix = &key[..plen];

                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let a = collect_kv(tree_with.prefix(prefix.to_vec(), s1, None));
                let b = collect_kv(tree_without.prefix(prefix.to_vec(), s2, None));
                assert_eq!(
                    a, b,
                    "op {i}: existing-key prefix scan mismatch for prefix {prefix:?} (from key {key:?})"
                );
            }

            Op::FlushCompactReopenCompact { setup } => {
                // Phase 1: flush and compact current data with small target
                // to produce many tables
                tree_with.flush_active_memtable(0).unwrap();
                tree_without.flush_active_memtable(0).unwrap();
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let _ = tree_with.major_compact(128, s1);
                let _ = tree_without.major_compact(128, s2);

                // Phase 2: reopen with another extractor
                current_setup = setup.clone();
                drop(tree_with);
                drop(tree_without);
                tree_with = open_tree(
                    &dir_with,
                    &seqno_with,
                    &vis_with,
                    Some(&current_setup),
                    bloom_bpk,
                    filter_partitioning,
                );
                tree_without = open_tree(
                    &dir_without,
                    &seqno_without,
                    &vis_without,
                    None,
                    bloom_bpk,
                    filter_partitioning,
                );
                snapshot_seqno_with = None;
                snapshot_seqno_without = None;
            }

            // ----- MVCC -----
            Op::TakeSnapshot => {
                snapshot_seqno_with = Some(vis_with.get());
                snapshot_seqno_without = Some(vis_without.get());
            }

            Op::SnapshotGet { key } => {
                let (Some(sw), Some(so)) = (snapshot_seqno_with, snapshot_seqno_without) else {
                    continue; // No snapshot taken yet, skip
                };
                let key = &key.0;
                let r1 = tree_with.get(key.as_slice(), sw).unwrap();
                let r2 = tree_without.get(key.as_slice(), so).unwrap();
                assert_eq!(
                    r1, r2,
                    "op {i}: snapshot point read mismatch for key {key:?} at seqno ({sw}, {so})"
                );
            }

            Op::SnapshotPrefixScan { prefix } => {
                let (Some(sw), Some(so)) = (snapshot_seqno_with, snapshot_seqno_without) else {
                    continue; // No snapshot taken yet, skip
                };
                let prefix = &prefix.0;
                let a = collect_kv(tree_with.prefix(prefix.clone(), sw, None));
                let b = collect_kv(tree_without.prefix(prefix.clone(), so, None));
                assert_eq!(
                    a, b,
                    "op {i}: snapshot prefix scan mismatch for {prefix:?} at seqno ({sw}, {so})"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AFL entry point
// ---------------------------------------------------------------------------

fn main() {
    fuzz!(|data: &[u8]| {
        let mut u = Unstructured::new(data);
        let Ok(input) = FuzzInput::arbitrary(&mut u) else {
            return;
        };

        // Limit op count so each iteration stays fast for AFL.
        if input.ops.is_empty() || input.ops.len() > 200 {
            return;
        }
        // Cap PrefixPingPong directions to avoid very slow iterations.
        for op in &input.ops {
            if let Op::PrefixPingPong { directions, .. } = op {
                if directions.len() > 50 {
                    return;
                }
            }
        }

        run_oracle_test(
            &input.setup,
            input.bpk.value(),
            &input.filter_partitioning,
            &input.ops,
        );
    });
}
