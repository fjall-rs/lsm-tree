// Property-based model test: compare lsm-tree against BTreeMap oracle.
//
// The oracle models MVCC using (key, Reverse(seqno)) ordering, where
// None values represent tombstones. This oracle only models point
// tombstones; range tombstones are tested separately in prop_range_tombstone.rs.

mod common;

use common::guard_to_kv;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// Simplified MVCC oracle without range tombstones.
/// Range tombstone testing is in prop_range_tombstone.rs.
#[derive(Debug, Clone)]
struct Oracle {
    /// (key, Reverse(seqno)) -> Some(value) for puts, None for tombstones.
    data: BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>,
}

impl Oracle {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>, seqno: u64) {
        self.data.insert((key, Reverse(seqno)), Some(value));
    }

    fn remove(&mut self, key: Vec<u8>, seqno: u64) {
        self.data.insert((key, Reverse(seqno)), None);
    }

    /// Point read: return the latest visible value at read_seqno.
    /// lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
    fn get(&self, key: &[u8], read_seqno: u64) -> Option<Vec<u8>> {
        if read_seqno == 0 {
            return None;
        }
        // Exclusive: find entries with seqno < read_seqno (i.e., <= read_seqno - 1)
        let start = (key.to_vec(), Reverse(read_seqno - 1));
        let end_inclusive = (key.to_vec(), Reverse(0));

        self.data
            .range(start..=end_inclusive)
            .take_while(|((k, _), _)| k == key)
            .map(|(_, val)| val.clone())
            .next()
            .flatten()
    }

    /// Full scan: return all visible (key, value) pairs at read_seqno, sorted by key.
    /// lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
    fn scan(&self, read_seqno: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        let mut last_key: Option<&Vec<u8>> = None;

        for ((key, Reverse(entry_seqno)), val) in &self.data {
            if *entry_seqno >= read_seqno {
                continue;
            }
            if last_key == Some(key) {
                continue;
            }
            last_key = Some(key);

            if let Some(value) = val {
                result.push((key.clone(), value.clone()));
            }
        }

        result
    }

    /// Prefix scan: return visible entries with given prefix at read_seqno.
    fn prefix_scan(&self, prefix: &[u8], read_seqno: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan(read_seqno)
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Op generation
// ---------------------------------------------------------------------------

/// Small key space to maximize collisions and test MVCC deduplication.
const KEY_SPACE: u8 = 16;

fn key_from_idx(idx: u8) -> Vec<u8> {
    vec![idx]
}

// NOTE: RemoveRange is excluded from this oracle because it only models
// point operations. Range tombstone semantics (including interaction with
// point tombstones across SSTs) are covered by prop_range_tombstone.rs
// and related regression tests instead.
#[derive(Debug, Clone)]
enum Op {
    Insert { key_idx: u8, value: Vec<u8> },
    Remove { key_idx: u8 },
    Flush,
    Compact,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 50% inserts
        5 => (0..KEY_SPACE, prop::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(key_idx, value)| Op::Insert { key_idx, value }),
        // 20% removes
        2 => (0..KEY_SPACE).prop_map(|key_idx| Op::Remove { key_idx }),
        // 20% flushes
        2 => Just(Op::Flush),
        // 10% compactions
        1 => Just(Op::Compact),
    ]
}

fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 10..100)
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

fn run_oracle_test(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let seqno_counter = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();
    let tree = Config::new(&tmpdir, seqno_counter.clone(), visible_seqno.clone())
        .open()
        .map_err(|e| TestCaseError::fail(format!("failed to open tree: {e}")))?;

    let mut oracle = Oracle::new();

    // Apply all ops.
    // Data seqnos come from the shared counter (as required by the API).
    // Internal operations (flush, compact) may also advance this counter via
    // upgrade_version when they do work, keeping SV seqnos and data seqnos
    // interleaved in those cases.
    for op in &ops {
        match op {
            Op::Insert { key_idx, value } => {
                let key = key_from_idx(*key_idx);
                let seqno = seqno_counter.next();
                oracle.insert(key.clone(), value.clone(), seqno);
                tree.insert(key, value.clone(), seqno);
                visible_seqno.fetch_max(seqno + 1);
            }
            Op::Remove { key_idx } => {
                let key = key_from_idx(*key_idx);
                let seqno = seqno_counter.next();
                oracle.remove(key.clone(), seqno);
                tree.remove(key, seqno);
                visible_seqno.fetch_max(seqno + 1);
            }
            Op::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(format!("flush failed: {e}")))?;
            }
            Op::Compact => {
                let gc_watermark = seqno_counter.get();
                tree.major_compact(common::COMPACTION_TARGET, gc_watermark)
                    .map_err(|e| TestCaseError::fail(format!("compact failed: {e}")))?;
            }
        }
    }

    // Verify point reads.
    // Use visible_seqno — it tracks the visibility watermark and won't
    // drift ahead of what the tree considers readable.
    let read_seqno = visible_seqno.get();
    for idx in 0..KEY_SPACE {
        let key = key_from_idx(idx);
        let expected = oracle.get(&key, read_seqno);
        let actual = tree
            .get(&key, read_seqno)
            .map_err(|e| TestCaseError::fail(format!("get failed: {e}")))?;

        prop_assert_eq!(
            actual.as_ref().map(|v| v.to_vec()),
            expected,
            "Point read mismatch for key {:?} at seqno {}",
            key,
            read_seqno,
        );
    }

    // Verify full scan.
    let expected_scan = oracle.scan(read_seqno);
    let actual_scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .iter(read_seqno, None)
        .map(guard_to_kv)
        .collect::<lsm_tree::Result<Vec<_>>>()
        .map_err(|e| TestCaseError::fail(format!("scan: {e}")))?;

    prop_assert_eq!(
        actual_scan.len(),
        expected_scan.len(),
        "Scan length mismatch: tree={}, oracle={}",
        actual_scan.len(),
        expected_scan.len(),
    );

    for (actual, expected) in actual_scan.iter().zip(expected_scan.iter()) {
        prop_assert_eq!(actual, expected, "Scan entry mismatch");
    }

    // Verify prefix scans for each possible prefix byte.
    // With single-byte keys each prefix matches exactly one key — this is
    // intentional: it validates the prefix() API contract and oracle agreement.
    // Multi-key prefix grouping is exercised by the db_bench prefixscan workload.
    for prefix_byte in 0..KEY_SPACE {
        let prefix = vec![prefix_byte];
        let expected_prefix = oracle.prefix_scan(&prefix, read_seqno);
        let actual_prefix: Vec<(Vec<u8>, Vec<u8>)> = tree
            .prefix(&prefix, read_seqno, None)
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(|e| TestCaseError::fail(format!("prefix scan: {e}")))?;

        prop_assert_eq!(
            actual_prefix.len(),
            expected_prefix.len(),
            "Prefix scan length mismatch for prefix {:?}",
            prefix,
        );

        for (actual, expected) in actual_prefix.iter().zip(expected_prefix.iter()) {
            prop_assert_eq!(
                actual,
                expected,
                "Prefix scan entry mismatch for prefix {:?}",
                prefix,
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// proptest
// ---------------------------------------------------------------------------

proptest! {
    // cases defaults to 256; CI overrides via PROPTEST_CASES=32
    #![proptest_config(ProptestConfig {
        fork: false,
        max_shrink_iters: 1000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_btreemap_oracle_correctness(ops in ops_strategy()) {
        run_oracle_test(ops)?;
    }
}
