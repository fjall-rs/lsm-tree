// Property-based model test: compare lsm-tree against BTreeMap oracle.
//
// The oracle models MVCC using (key, Reverse(seqno)) ordering, where
// None values represent point tombstones. Range tombstones are not modeled
// here; they are covered by dedicated tests in range_tombstone.rs.
//
// Keys are 2–4 bytes with a small first-byte alphabet (0..4), so multiple
// keys naturally share leading bytes and prefix scans exercise grouping,
// boundary, and multi-entry result semantics.

mod common;

use common::guard_to_kv;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// Key with reverse-ordered seqno for MVCC lookup via BTreeMap range queries.
type MvccKey = (Vec<u8>, Reverse<u64>);

/// Simplified MVCC oracle without range tombstones.
/// Range tombstone testing lives in tests/range_tombstone.rs and
/// tests/range_tombstone_ephemeral.rs.
#[derive(Debug, Clone)]
struct Oracle {
    /// (key, Reverse(seqno)) -> Some(value) for puts, None for tombstones.
    data: BTreeMap<MvccKey, Option<Vec<u8>>>,
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
            .next()
            .filter(|((k, _), _)| k == key)
            .and_then(|(_, val)| val.clone())
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

    /// Collect all distinct first bytes present in the oracle's key space.
    fn distinct_prefixes(&self) -> Vec<Vec<u8>> {
        let mut seen = std::collections::BTreeSet::new();
        for (key, _) in self.data.keys() {
            if let Some(&b) = key.first() {
                seen.insert(b);
            }
        }
        seen.into_iter().map(|b| vec![b]).collect()
    }
}

// ---------------------------------------------------------------------------
// Key generation
// ---------------------------------------------------------------------------

/// Small first-byte alphabet so multiple keys share a common prefix byte.
const PREFIX_ALPHABET: u8 = 4;

/// Generate a multi-byte key: first byte from a small alphabet (0..4),
/// followed by 1–3 suffix bytes drawn from the full u8 range.
/// This guarantees natural prefix grouping while preserving collision
/// potential on the full key.
fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
    (
        0..PREFIX_ALPHABET,
        prop::collection::vec(any::<u8>(), 1..=3),
    )
        .prop_map(|(prefix_byte, suffix)| {
            let mut key = Vec::with_capacity(1 + suffix.len());
            key.push(prefix_byte);
            key.extend_from_slice(&suffix);
            key
        })
}

// NOTE: RemoveRange is excluded from this oracle because it only models
// point operations. Range tombstone semantics (including interaction with
// point tombstones across SSTs) are covered by range_tombstone.rs,
// range_tombstone_ephemeral.rs, and related regression tests instead.
#[derive(Debug, Clone)]
enum Op {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    Flush,
    Compact,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 50% inserts
        5 => (key_strategy(), prop::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(key, value)| Op::Insert { key, value }),
        // 20% removes
        2 => key_strategy().prop_map(|key| Op::Remove { key }),
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
    let tree = Config::new(
        &tmpdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .map_err(|e| TestCaseError::fail(format!("failed to open tree: {e}")))?;

    let mut oracle = Oracle::new();
    let mut seqno: u64 = 1;
    let mut all_keys: Vec<Vec<u8>> = Vec::new();

    // Apply all ops.
    for op in &ops {
        match op {
            Op::Insert { key, value } => {
                oracle.insert(key.clone(), value.clone(), seqno);
                tree.insert(key, value.clone(), seqno);
                all_keys.push(key.clone());
                seqno += 1;
            }
            Op::Remove { key } => {
                oracle.remove(key.clone(), seqno);
                tree.remove(key, seqno);
                all_keys.push(key.clone());
                seqno += 1;
            }
            Op::Flush => {
                tree.flush_active_memtable(0)
                    .map_err(|e| TestCaseError::fail(format!("flush failed: {e}")))?;
            }
            Op::Compact => {
                tree.major_compact(common::COMPACTION_TARGET, seqno)
                    .map_err(|e| TestCaseError::fail(format!("compact failed: {e}")))?;
            }
        }
    }

    // Verify point reads for every key that was ever inserted or removed.
    let read_seqno = seqno;
    all_keys.sort();
    all_keys.dedup();

    for key in &all_keys {
        let expected = oracle.get(key, read_seqno);
        let actual = tree
            .get(key, read_seqno)
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

    // Verify prefix scans for every distinct first byte observed.
    // With multi-byte keys and PREFIX_ALPHABET=4, each prefix typically
    // groups multiple keys, exercising grouping and boundary semantics.
    for prefix in oracle.distinct_prefixes() {
        let expected_prefix = oracle.prefix_scan(&prefix, read_seqno);
        let actual_prefix: Vec<(Vec<u8>, Vec<u8>)> = tree
            .prefix(&prefix, read_seqno, None)
            .map(guard_to_kv)
            .collect::<lsm_tree::Result<Vec<_>>>()
            .map_err(|e| TestCaseError::fail(format!("prefix scan: {e}")))?;

        prop_assert_eq!(
            actual_prefix.len(),
            expected_prefix.len(),
            "Prefix scan length mismatch for prefix {:?}: tree={}, oracle={}",
            prefix,
            actual_prefix.len(),
            expected_prefix.len(),
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
    // 32 cases: keeps CI runtime bounded across the 3-OS nextest matrix.
    // PROPTEST_CASES env var can still override at runtime.
    // fork disabled: rusty-fork cannot re-exec under QEMU cross-compilation.
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 1000,
        fork: false,
        timeout: 0,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_btreemap_oracle_correctness(ops in ops_strategy()) {
        run_oracle_test(ops)?;
    }
}
