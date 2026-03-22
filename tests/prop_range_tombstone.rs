// Property-based test focused on range tombstone correctness.
//
// Generates sequences of inserts and range deletes interleaved with
// flush and compact operations, then verifies that point reads and scans
// match a BTreeMap oracle.

mod common;

use common::guard_to_kv;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Minimal oracle (range tombstone focused)
// ---------------------------------------------------------------------------

struct RtOracle {
    data: BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>,
    range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

impl RtOracle {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            range_tombstones: Vec::new(),
        }
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>, seqno: u64) {
        self.data.insert((key, Reverse(seqno)), Some(value));
    }

    fn remove_range(&mut self, start: Vec<u8>, end: Vec<u8>, seqno: u64) {
        if start < end {
            self.range_tombstones.push((start, end, seqno));
        }
    }

    fn is_range_deleted(&self, key: &[u8], kv_seqno: u64, read_seqno: u64) -> bool {
        self.range_tombstones.iter().any(|(start, end, rt_seqno)| {
            *rt_seqno < read_seqno
                && key >= start.as_slice()
                && key < end.as_slice()
                && *rt_seqno > kv_seqno
        })
    }

    // lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
    fn get(&self, key: &[u8], read_seqno: u64) -> Option<Vec<u8>> {
        if read_seqno == 0 {
            return None;
        }

        let start = (key.to_vec(), Reverse(read_seqno - 1));
        let end = (key.to_vec(), Reverse(0));

        for ((k, Reverse(entry_seqno)), val) in self.data.range(start..=end) {
            if k != key {
                break;
            }
            if self.is_range_deleted(key, *entry_seqno, read_seqno) {
                return None;
            }
            return val.clone();
        }
        None
    }

    // lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
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

            if self.is_range_deleted(key, *entry_seqno, read_seqno) {
                continue;
            }

            if let Some(value) = val {
                result.push((key.clone(), value.clone()));
            }
        }

        result
    }
}

// ---------------------------------------------------------------------------
// Op generation focused on range tombstone edge cases
// ---------------------------------------------------------------------------

const KEY_SPACE: u8 = 12;

fn key_from_idx(idx: u8) -> Vec<u8> {
    vec![idx]
}

#[derive(Debug, Clone)]
enum RtOp {
    /// Insert a key with value.
    Insert { key_idx: u8, value: u8 },
    /// Delete range [lo, hi+1).
    DeleteRange { lo: u8, hi: u8 },
    /// Flush memtable to disk.
    Flush,
    /// Major compaction.
    Compact,
}

fn rt_op_strategy() -> impl Strategy<Value = RtOp> {
    prop_oneof![
        // 40% inserts
        4 => (0..KEY_SPACE, any::<u8>())
            .prop_map(|(key_idx, value)| RtOp::Insert { key_idx, value }),
        // 30% range deletes (high proportion to stress RT path)
        3 => (0..KEY_SPACE, 0..KEY_SPACE)
            .prop_map(|(a, b)| {
                let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                RtOp::DeleteRange { lo, hi }
            }),
        // 20% flushes
        2 => Just(RtOp::Flush),
        // 10% compactions
        1 => Just(RtOp::Compact),
    ]
}

fn rt_ops_strategy() -> impl Strategy<Value = Vec<RtOp>> {
    prop::collection::vec(rt_op_strategy(), 20..150)
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

fn run_rt_test(ops: Vec<RtOp>) -> Result<(), TestCaseError> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let tree = Config::new(
        &tmpdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .map_err(|e| TestCaseError::fail(format!("open: {e}")))?;

    let mut oracle = RtOracle::new();
    let mut seqno: u64 = 1;

    for op in &ops {
        match op {
            RtOp::Insert { key_idx, value } => {
                let key = key_from_idx(*key_idx);
                let val = vec![*value; 8];
                oracle.insert(key.clone(), val.clone(), seqno);
                tree.insert(key, val, seqno);
                seqno += 1;
            }
            RtOp::DeleteRange { lo, hi } => {
                let start = key_from_idx(*lo);
                let end = key_from_idx(hi.saturating_add(1));
                if start < end {
                    oracle.remove_range(start.clone(), end.clone(), seqno);
                    tree.remove_range(&start, &end, seqno);
                    seqno += 1;
                }
            }
            RtOp::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(format!("flush: {e}")))?;
            }
            RtOp::Compact => {
                tree.major_compact(common::COMPACTION_TARGET, seqno)
                    .map_err(|e| TestCaseError::fail(format!("compact: {e}")))?;
            }
        }
    }

    // Verify at current seqno.
    let read_seqno = seqno;

    // Point reads.
    for idx in 0..KEY_SPACE {
        let key = key_from_idx(idx);
        let expected = oracle.get(&key, read_seqno);
        let actual = tree
            .get(&key, read_seqno)
            .map_err(|e| TestCaseError::fail(format!("get: {e}")))?;

        prop_assert_eq!(
            actual.as_ref().map(|v| v.to_vec()),
            expected,
            "Point read mismatch for key [{}] at seqno {}",
            idx,
            read_seqno,
        );
    }

    // Full scan.
    let expected_scan = oracle.scan(read_seqno);
    let actual_scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .iter(read_seqno, None)
        .map(guard_to_kv)
        .collect::<lsm_tree::Result<Vec<_>>>()
        .map_err(|e| TestCaseError::fail(format!("scan: {e}")))?;

    prop_assert_eq!(
        actual_scan.len(),
        expected_scan.len(),
        "Scan length: tree={}, oracle={}",
        actual_scan.len(),
        expected_scan.len(),
    );

    for (i, (actual, expected)) in actual_scan.iter().zip(expected_scan.iter()).enumerate() {
        prop_assert_eq!(actual, expected, "Scan entry {} mismatch", i);
    }

    // Reverse scan.
    let expected_rev: Vec<_> = expected_scan.into_iter().rev().collect();
    let actual_rev: Vec<(Vec<u8>, Vec<u8>)> = tree
        .iter(read_seqno, None)
        .rev()
        .map(guard_to_kv)
        .collect::<lsm_tree::Result<Vec<_>>>()
        .map_err(|e| TestCaseError::fail(format!("rev scan: {e}")))?;

    prop_assert_eq!(
        actual_rev.len(),
        expected_rev.len(),
        "Reverse scan length mismatch",
    );

    for (i, (actual, expected)) in actual_rev.iter().zip(expected_rev.iter()).enumerate() {
        prop_assert_eq!(actual, expected, "Reverse scan entry {} mismatch", i);
    }

    Ok(())
}

proptest! {
    // cases defaults to 256; CI overrides via PROPTEST_CASES=32
    #![proptest_config(ProptestConfig {
        fork: false,
        max_shrink_iters: 1000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_range_tombstone_correctness(ops in rt_ops_strategy()) {
        run_rt_test(ops)?;
    }
}
