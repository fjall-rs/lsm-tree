#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry, PinningPolicy};
use lsm_tree::prefix::{
    FixedLengthExtractor, FixedPrefixExtractor, FullKeyExtractor, SharedPrefixExtractor,
};
use lsm_tree::{AbstractTree, AnyTree, Guard, IterGuardImpl, SequenceNumberCounter};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Structured input derived from AFL's raw bytes via Arbitrary
// ---------------------------------------------------------------------------

#[derive(Arbitrary, Debug, Clone)]
enum ExtractorChoice {
    FixedLength1,
    FixedLength2,
    FixedLength3,
    FixedLength4,
    FixedPrefix1,
    FixedPrefix2,
    FixedPrefix3,
    FixedPrefix4,
    FullKey,
}

impl ExtractorChoice {
    fn into_extractor(&self) -> SharedPrefixExtractor {
        match self {
            Self::FixedLength1 => Arc::new(FixedLengthExtractor::new(1)),
            Self::FixedLength2 => Arc::new(FixedLengthExtractor::new(2)),
            Self::FixedLength3 => Arc::new(FixedLengthExtractor::new(3)),
            Self::FixedLength4 => Arc::new(FixedLengthExtractor::new(4)),
            Self::FixedPrefix1 => Arc::new(FixedPrefixExtractor::new(1)),
            Self::FixedPrefix2 => Arc::new(FixedPrefixExtractor::new(2)),
            Self::FixedPrefix3 => Arc::new(FixedPrefixExtractor::new(3)),
            Self::FixedPrefix4 => Arc::new(FixedPrefixExtractor::new(4)),
            Self::FullKey => Arc::new(FullKeyExtractor),
        }
    }
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

/// A key with first byte drawn from a small alphabet (0..8) and bounded
/// length (1..=9). This ensures keys cluster into a small number of prefix
/// groups, so prefix scans and filter lookups frequently hit real data.
///
/// With extractors up to length 4 and keys as short as 1 byte, AFL naturally
/// explores both in-domain and out-of-domain keys.
#[derive(Debug, Clone)]
struct ClusteredKey(Vec<u8>);

impl<'a> Arbitrary<'a> for ClusteredKey {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len: usize = u.int_in_range(1..=9)?;
        let first_byte: u8 = u.int_in_range(0..=7)?;
        let mut key = Vec::with_capacity(len);
        key.push(first_byte);
        for _ in 1..len {
            key.push(u8::arbitrary(u)?);
        }
        Ok(ClusteredKey(key))
    }
}

/// A prefix with length 0..=3, each byte from the same small alphabet (0..8).
/// Likely to match actual key prefixes since keys share the same first-byte space.
/// Length 0 = empty prefix, which maps to an unbounded full scan.
#[derive(Debug, Clone)]
struct ClusteredPrefix(Vec<u8>);

impl<'a> Arbitrary<'a> for ClusteredPrefix {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len: usize = u.int_in_range(0..=3)?;
        let mut prefix = Vec::with_capacity(len);
        for _ in 0..len {
            prefix.push(u.int_in_range(0..=7)?);
        }
        Ok(ClusteredPrefix(prefix))
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
    Flush,
    Compact,
    MajorCompact,
    /// Clean close + reopen with the same extractor.
    Reopen,
    /// Close + reopen with a DIFFERENT extractor. Tests the
    /// `prefix_filter_allowed()` compatibility gating: old tables keep their
    /// old extractor metadata, new flushes use the new extractor.
    ReopenNewExtractor {
        new_extractor: ExtractorChoice,
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
    /// Unbounded iteration endpoints. Tests first_key_value / last_key_value
    /// which use `range(..)` internally — prefix filters should not interfere
    /// with unbounded scans.
    FirstKV,
    LastKV,

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
    extractor: ExtractorChoice,
    bpk: BpkChoice,
    filter_partitioning: FilterPartitioningChoice,
    ops: Vec<Op>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn collect_kv(iter: impl Iterator<Item = IterGuardImpl>) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.filter_map(|g| {
        let (k, v) = g.into_inner().ok()?;
        Some((k.to_vec(), v.to_vec()))
    })
    .collect()
}

fn open_tree(
    dir: &tempfile::TempDir,
    seqno: &SequenceNumberCounter,
    vis: &SequenceNumberCounter,
    extractor: Option<SharedPrefixExtractor>,
    bloom_bpk: f32,
    filter_partitioning: &FilterPartitioningChoice,
) -> AnyTree {
    let mut config =
        lsm_tree::Config::new(dir, seqno.clone(), vis.clone()).filter_policy(FilterPolicy::all(
            FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(bloom_bpk)),
        ));
    if let Some(ex) = extractor {
        config = config.prefix_extractor(ex);
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
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
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
    initial_extractor: &ExtractorChoice,
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

    // Current extractor for tree_with (may change via ReopenNewExtractor).
    let mut current_extractor = initial_extractor.into_extractor();

    let mut tree_with = open_tree(
        &dir_with,
        &seqno_with,
        &vis_with,
        Some(current_extractor.clone()),
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
            Op::Flush => {
                tree_with.flush_active_memtable(0).unwrap();
                tree_without.flush_active_memtable(0).unwrap();
            }

            Op::Compact => {
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let _ = tree_with.compact(compaction_strategy.clone(), s1);
                let _ = tree_without.compact(compaction_strategy.clone(), s2);
            }

            Op::MajorCompact => {
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let _ = tree_with.major_compact(4_096, s1);
                let _ = tree_without.major_compact(4_096, s2);
            }

            Op::Reopen => {
                drop(tree_with);
                drop(tree_without);
                tree_with = open_tree(
                    &dir_with,
                    &seqno_with,
                    &vis_with,
                    Some(current_extractor.clone()),
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
            }

            Op::ReopenNewExtractor { new_extractor } => {
                current_extractor = new_extractor.into_extractor();
                drop(tree_with);
                drop(tree_without);
                tree_with = open_tree(
                    &dir_with,
                    &seqno_with,
                    &vis_with,
                    Some(current_extractor.clone()),
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

                    let kv_with = item_with.and_then(|g| g.into_inner().ok());
                    let kv_without = item_without.and_then(|g| g.into_inner().ok());

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

            Op::FirstKV => {
                let s1 = vis_with.get();
                let s2 = vis_without.get();
                let r1 = tree_with
                    .first_key_value(s1, None)
                    .and_then(|g| g.into_inner().ok());
                let r2 = tree_without
                    .first_key_value(s2, None)
                    .and_then(|g| g.into_inner().ok());
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
                let r1 = tree_with
                    .last_key_value(s1, None)
                    .and_then(|g| g.into_inner().ok());
                let r2 = tree_without
                    .last_key_value(s2, None)
                    .and_then(|g| g.into_inner().ok());
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
            &input.extractor,
            input.bpk.value(),
            &input.filter_partitioning,
            &input.ops,
        );
    });
}
