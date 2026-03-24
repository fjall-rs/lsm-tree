// Property-based test for MVCC snapshot consistency.
//
// Writes multiple versions of keys at different seqnos, then reads
// at each historical seqno to verify snapshot isolation. Compares
// forward and reverse iterators for consistency.

mod common;

use common::guard_to_kv;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

struct MvccOracle {
    data: BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>,
}

impl MvccOracle {
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

    // lsm-tree uses exclusive upper bound: entry_seqno < read_seqno.
    fn get(&self, key: &[u8], read_seqno: u64) -> Option<Vec<u8>> {
        if read_seqno == 0 {
            return None;
        }

        let start = (key.to_vec(), Reverse(read_seqno - 1));
        let end = (key.to_vec(), Reverse(0));

        self.data
            .range(start..=end)
            .next()
            .filter(|((k, _), _)| k == key)
            .and_then(|(_, val)| val.clone())
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

            if let Some(value) = val {
                result.push((key.clone(), value.clone()));
            }
        }

        result
    }
}

// ---------------------------------------------------------------------------
// Op generation
// ---------------------------------------------------------------------------

const KEY_SPACE: u8 = 8;

fn key_from_idx(idx: u8) -> Vec<u8> {
    vec![idx]
}

#[derive(Debug, Clone)]
enum MvccOp {
    Insert { key_idx: u8, value: u8 },
    Remove { key_idx: u8 },
    Flush,
}

fn mvcc_op_strategy() -> impl Strategy<Value = MvccOp> {
    prop_oneof![
        5 => (0..KEY_SPACE, any::<u8>())
            .prop_map(|(key_idx, value)| MvccOp::Insert { key_idx, value }),
        2 => (0..KEY_SPACE).prop_map(|key_idx| MvccOp::Remove { key_idx }),
        1 => Just(MvccOp::Flush),
    ]
}

fn mvcc_ops_strategy() -> impl Strategy<Value = Vec<MvccOp>> {
    prop::collection::vec(mvcc_op_strategy(), 10..100)
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

fn run_mvcc_test(ops: Vec<MvccOp>) -> Result<(), TestCaseError> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let tree = Config::new(
        &tmpdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .map_err(|e| TestCaseError::fail(format!("open: {e}")))?;

    let mut oracle = MvccOracle::new();
    let mut seqno: u64 = 1;
    let mut snapshot_seqnos: Vec<u64> = Vec::new();

    // Apply ops, recording snapshot points at each write.
    for op in &ops {
        match op {
            MvccOp::Insert { key_idx, value } => {
                let key = key_from_idx(*key_idx);
                let val = vec![*value; 4];
                oracle.insert(key.clone(), val.clone(), seqno);
                tree.insert(key, val, seqno);
                // Record seqno + 1 so reads at this snapshot see the write
                // (lsm-tree uses exclusive upper bound: entry_seqno < read_seqno)
                snapshot_seqnos.push(seqno + 1);
                seqno += 1;
            }
            MvccOp::Remove { key_idx } => {
                let key = key_from_idx(*key_idx);
                oracle.remove(key.clone(), seqno);
                tree.remove(key, seqno);
                snapshot_seqnos.push(seqno + 1);
                seqno += 1;
            }
            MvccOp::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(format!("flush: {e}")))?;
            }
        }
    }

    // Sample snapshot points to keep verification time bounded.
    // Always include the last snapshot to verify the final tree state.
    // If no writes occurred (all ops were Flush), verify the empty state at seqno 1.
    let check_points: Vec<u64> = if snapshot_seqnos.is_empty() {
        vec![1]
    } else if snapshot_seqnos.len() <= 20 {
        snapshot_seqnos.clone()
    } else {
        // Ceiling division so step >= 2 when len > 20, bounding to ~20 checks.
        let step = (snapshot_seqnos.len() + 19) / 20;
        let mut points: Vec<u64> = snapshot_seqnos.iter().step_by(step).copied().collect();
        if let Some(&last) = snapshot_seqnos.last() {
            if points.last() != Some(&last) {
                points.push(last);
            }
        }
        points
    };

    // Verify at each historical snapshot point.
    for &snap_seqno in &check_points {
        // Point reads.
        for idx in 0..KEY_SPACE {
            let key = key_from_idx(idx);
            let expected = oracle.get(&key, snap_seqno);
            let actual = tree
                .get(&key, snap_seqno)
                .map_err(|e| TestCaseError::fail(format!("get: {e}")))?;

            prop_assert_eq!(
                actual.as_ref().map(|v| v.to_vec()),
                expected,
                "Point read mismatch: key=[{}] seqno={}",
                idx,
                snap_seqno,
            );
        }

        // Forward scan.
        let expected_scan = oracle.scan(snap_seqno);
        let actual_scan: Vec<(Vec<u8>, Vec<u8>)> = tree
            .iter(snap_seqno, None)
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(|e| TestCaseError::fail(format!("scan: {e}")))?;

        prop_assert_eq!(
            actual_scan.len(),
            expected_scan.len(),
            "Scan length mismatch at seqno {}: tree={}, oracle={}",
            snap_seqno,
            actual_scan.len(),
            expected_scan.len(),
        );

        for (i, (actual, expected)) in actual_scan.iter().zip(expected_scan.iter()).enumerate() {
            prop_assert_eq!(
                actual,
                expected,
                "Scan entry {} mismatch at seqno {}",
                i,
                snap_seqno,
            );
        }

        // Reverse scan must match forward scan reversed.
        let actual_rev: Vec<(Vec<u8>, Vec<u8>)> = tree
            .iter(snap_seqno, None)
            .rev()
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(|e| TestCaseError::fail(format!("rev scan: {e}")))?;

        let expected_rev: Vec<_> = actual_scan.into_iter().rev().collect();

        prop_assert_eq!(
            actual_rev.len(),
            expected_rev.len(),
            "Reverse scan length mismatch at seqno {}",
            snap_seqno,
        );

        for (i, (actual, expected)) in actual_rev.iter().zip(expected_rev.iter()).enumerate() {
            prop_assert_eq!(
                actual,
                expected,
                "Reverse scan entry {} mismatch at seqno {}",
                i,
                snap_seqno,
            );
        }
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
    fn prop_mvcc_snapshot_consistency(ops in mvcc_ops_strategy()) {
        run_mvcc_test(ops)?;
    }
}
