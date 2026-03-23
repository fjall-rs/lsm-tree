use lsm_tree::compaction::{Leveled, SizeTiered};
use lsm_tree::{
    // Guard import is required: into_inner() is a trait method from IterGuard (re-exported as Guard)
    AbstractTree,
    Config,
    Guard as _,
    MergeOperator,
    SeqNo,
    SequenceNumberCounter,
    SharedComparator,
    UserComparator,
    UserValue,
};
use std::cmp::Ordering;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Custom comparators
// ---------------------------------------------------------------------------

/// Reverses the default lexicographic byte ordering.
struct ReverseComparator;

impl UserComparator for ReverseComparator {
    fn name(&self) -> &'static str {
        "reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

/// Orders u64 keys stored as big-endian bytes numerically.
///
/// NOTE: For 8-byte big-endian encoded u64s, numeric ordering is identical to
/// lexicographic byte ordering. This means these tests exercise the compaction
/// and merge code paths but do NOT stress non-lexicographic ordering.
/// The `ReverseComparator` tests below cover that case.
struct U64BigEndianComparator;

impl UserComparator for U64BigEndianComparator {
    fn name(&self) -> &'static str {
        "u64-big-endian"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() == 8 && b.len() == 8 {
            let a_u64 = u64::from_be_bytes(a.try_into().unwrap());
            let b_u64 = u64::from_be_bytes(b.try_into().unwrap());
            a_u64.cmp(&b_u64)
        } else {
            // Fallback for non-8-byte keys (e.g. internal metadata). All test
            // keys are 8-byte u64s so this branch doesn't affect test ordering.
            // Matches the pattern in tests/custom_comparator.rs.
            a.cmp(b)
        }
    }
}

/// Simple i64 counter merge operator: base + sum(operands).
struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut counter: i64 = match base_value {
            Some(bytes) if bytes.len() == 8 => {
                i64::from_le_bytes(bytes.try_into().expect("checked length"))
            }
            Some(_) => return Err(lsm_tree::Error::MergeOperator),
            None => 0,
        };

        for operand in operands {
            if operand.len() != 8 {
                return Err(lsm_tree::Error::MergeOperator);
            }
            counter += i64::from_le_bytes((*operand).try_into().expect("checked length"));
        }

        Ok(counter.to_le_bytes().to_vec().into())
    }
}

// Mirrors tests/merge_operator.rs helper — unwrap is intentional in test code
// to surface failures as panics with full backtrace context.
fn get_counter(tree: &lsm_tree::AnyTree, key: &[u8], seqno: u64) -> Option<i64> {
    tree.get(key, seqno)
        .unwrap()
        .map(|v| i64::from_le_bytes((*v).try_into().unwrap()))
}

// ===========================================================================
// Section 1: Compaction with U64BigEndianComparator
//
// These exercise the compaction code path (flush → compact → verify) with a
// custom comparator. Because BE-u64 ordering matches lexicographic byte
// ordering, they pass without triggering the Run::push() sorting bug.
// ===========================================================================

#[test]
fn u64_comparator_leveled_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    // 3 flushes with interleaved keys
    for &k in &[500u64, 100, 1000] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    for &k in &[50u64, 750, 250] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    for &k in &[1u64, 42, 999] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    tree.compact(Arc::new(Leveled::default()), SeqNo::MAX)?;

    let items: Vec<u64> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 42, 50, 100, 250, 500, 750, 999, 1000]);

    // Point reads work after compaction
    for &k in &[1u64, 42, 50, 100, 250, 500, 750, 999, 1000] {
        assert!(
            tree.get(k.to_be_bytes().as_ref(), SeqNo::MAX)?.is_some(),
            "point read failed for key {k} after leveled compaction"
        );
    }

    Ok(())
}

#[test]
fn u64_comparator_size_tiered_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    for &k in &[500u64, 100, 1000] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    for &k in &[50u64, 750, 250] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    for &k in &[1u64, 42, 999] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    tree.compact(Arc::new(SizeTiered::default()), SeqNo::MAX)?;

    let items: Vec<u64> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 42, 50, 100, 250, 500, 750, 999, 1000]);

    Ok(())
}

#[test]
fn u64_comparator_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    for &k in &[500u64, 100, 1000] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    for &k in &[50u64, 750, 1] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    assert!(
        tree.table_count() >= 2,
        "need multiple SSTs before compaction"
    );
    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    assert_eq!(
        1,
        tree.table_count(),
        "major compaction should merge into 1 SST"
    );

    let items: Vec<u64> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 50, 100, 500, 750, 1000]);

    Ok(())
}

#[test]
fn u64_comparator_compaction_with_tombstones() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    for &k in &[10u64, 20, 30, 40, 50] {
        tree.insert(k.to_be_bytes(), format!("v{k}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // Delete even keys
    tree.remove(20u64.to_be_bytes(), seqno.next());
    tree.remove(40u64.to_be_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    let items: Vec<u64> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![10, 30, 50]);

    assert!(tree
        .get(20u64.to_be_bytes().as_ref(), SeqNo::MAX)?
        .is_none());
    assert!(tree
        .get(40u64.to_be_bytes().as_ref(), SeqNo::MAX)?
        .is_none());

    Ok(())
}

// ===========================================================================
// Section 2: Compaction with ReverseComparator
//
// These tests verify that compaction-merged SSTs retain correct ordering
// under a genuinely non-lexicographic comparator.
//
// NOTE: These tests also serve as regression coverage for a historical bug
// where Run::push() in src/version/run.rs sorted tables by min key using
// lexicographic .cmp() instead of the custom comparator, which caused
// incorrect inter-SST ordering after compaction and affected KeyRange
// overlap checks in key_range.rs and range_overlap_indexes() in
// version/run.rs.
// ===========================================================================

#[test]
fn reverse_comparator_leveled_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    for c in b'a'..=b'i' {
        tree.insert(&[c], format!("v_{}", c as char), seqno.next());
        // Flush every 3 keys to create multiple SSTs
        if (c - b'a' + 1) % 3 == 0 {
            tree.flush_active_memtable(0)?;
        }
    }

    tree.compact(Arc::new(Leveled::default()), SeqNo::MAX)?;

    // Reverse order: i, h, g, f, e, d, c, b, a
    let items: Vec<String> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["i", "h", "g", "f", "e", "d", "c", "b", "a"]);

    Ok(())
}

#[test]
fn reverse_comparator_size_tiered_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    for c in b'a'..=b'i' {
        tree.insert(&[c], format!("v_{}", c as char), seqno.next());
        if (c - b'a' + 1) % 3 == 0 {
            tree.flush_active_memtable(0)?;
        }
    }

    tree.compact(Arc::new(SizeTiered::default()), SeqNo::MAX)?;

    let items: Vec<String> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["i", "h", "g", "f", "e", "d", "c", "b", "a"]);

    Ok(())
}

#[test]
fn reverse_comparator_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", seqno.next());
    tree.insert("b", "val_b", seqno.next());
    tree.insert("c", "val_c", seqno.next());
    tree.flush_active_memtable(0)?;

    tree.insert("d", "val_d", seqno.next());
    tree.insert("e", "val_e", seqno.next());
    tree.flush_active_memtable(0)?;

    assert!(
        tree.table_count() >= 2,
        "need multiple SSTs before compaction"
    );
    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    assert_eq!(
        1,
        tree.table_count(),
        "major compaction should merge into 1 SST"
    );

    let items: Vec<String> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["e", "d", "c", "b", "a"]);

    Ok(())
}

#[test]
fn reverse_comparator_compaction_with_updates() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "old_a", seqno.next());
    tree.insert("b", "old_b", seqno.next());
    tree.insert("c", "old_c", seqno.next());
    tree.flush_active_memtable(0)?;

    tree.insert("b", "new_b", seqno.next());
    tree.remove("a", seqno.next());
    tree.insert("d", "val_d", seqno.next());
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // Reverse order of surviving keys: d, c, b
    let items: Vec<(String, String)> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![
            ("d".into(), "val_d".into()),
            ("c".into(), "old_c".into()),
            ("b".into(), "new_b".into()),
        ]
    );

    assert!(tree.get("a", SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
#[ignore = "range bounds interpretation for reverse comparator (#116)"]
fn reverse_comparator_range_scan_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .open()?;

    for c in b'a'..=b'f' {
        tree.insert(&[c], format!("v_{}", c as char), seqno.next());
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // Reverse order: f, e, d, c, b, a — range "e"..="b" should yield e, d, c, b
    let items: Vec<String> = tree
        .range("e"..="b", SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["e", "d", "c", "b"]);

    Ok(())
}

// ===========================================================================
// Section 3: Merge operator + U64BigEndianComparator through compaction
//
// These exercise merge operand resolution (resolve_merge_get) through the
// compaction stream with a custom comparator.
// ===========================================================================

#[test]
fn u64_comparator_merge_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    tree.insert(100u64.to_be_bytes(), 10_i64.to_le_bytes(), seqno.next());
    tree.merge(100u64.to_be_bytes(), 5_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some(15),
        get_counter(&tree, &100u64.to_be_bytes(), SeqNo::MAX)
    );

    Ok(())
}

#[test]
fn u64_comparator_merge_across_flush_boundary() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    // Base + first operand in SST
    tree.insert(100u64.to_be_bytes(), 100_i64.to_le_bytes(), seqno.next());
    tree.merge(100u64.to_be_bytes(), 10_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // More operands in active memtable
    tree.merge(100u64.to_be_bytes(), 20_i64.to_le_bytes(), seqno.next());
    tree.merge(100u64.to_be_bytes(), 30_i64.to_le_bytes(), seqno.next());

    assert_eq!(
        Some(160),
        get_counter(&tree, &100u64.to_be_bytes(), SeqNo::MAX)
    );

    Ok(())
}

#[test]
fn u64_comparator_merge_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    let keys = [500u64, 100, 1000];

    // Base values
    for &k in &keys {
        tree.insert(k.to_be_bytes(), 10_i64.to_le_bytes(), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // Merge operands
    for &k in &keys {
        tree.merge(k.to_be_bytes(), (k as i64).to_le_bytes(), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // Verify merged values: base(10) + key_value
    assert_eq!(
        Some(110),
        get_counter(&tree, &100u64.to_be_bytes(), SeqNo::MAX)
    );
    assert_eq!(
        Some(510),
        get_counter(&tree, &500u64.to_be_bytes(), SeqNo::MAX)
    );
    assert_eq!(
        Some(1010),
        get_counter(&tree, &1000u64.to_be_bytes(), SeqNo::MAX)
    );

    // Iteration in numeric order: 100, 500, 1000
    let items: Vec<(u64, i64)> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                u64::from_be_bytes(k[..8].try_into().unwrap()),
                i64::from_le_bytes((*v).try_into().unwrap()),
            )
        })
        .collect();

    assert_eq!(items, vec![(100, 110), (500, 510), (1000, 1010)]);

    Ok(())
}

#[test]
fn u64_comparator_merge_with_tombstone_and_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    // Base value, then delete, then merge (post-tombstone)
    tree.insert(100u64.to_be_bytes(), 50_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.remove(100u64.to_be_bytes(), seqno.next());
    tree.merge(100u64.to_be_bytes(), 7_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Before compaction: merge after tombstone → base=None → 7
    assert_eq!(
        Some(7),
        get_counter(&tree, &100u64.to_be_bytes(), SeqNo::MAX)
    );

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // After compaction: same result
    assert_eq!(
        Some(7),
        get_counter(&tree, &100u64.to_be_bytes(), SeqNo::MAX)
    );

    Ok(())
}

#[test]
fn u64_comparator_merge_multiple_keys_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    // Multiple keys with interleaved base values and merge operands across flushes
    tree.insert(10u64.to_be_bytes(), 100_i64.to_le_bytes(), seqno.next());
    tree.insert(30u64.to_be_bytes(), 300_i64.to_le_bytes(), seqno.next());
    tree.merge(10u64.to_be_bytes(), 1_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.merge(10u64.to_be_bytes(), 2_i64.to_le_bytes(), seqno.next());
    tree.merge(30u64.to_be_bytes(), 3_i64.to_le_bytes(), seqno.next());
    tree.insert(20u64.to_be_bytes(), 200_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // Verify all keys in numeric order with correctly merged values
    let items: Vec<(u64, i64)> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                u64::from_be_bytes(k[..8].try_into().unwrap()),
                i64::from_le_bytes((*v).try_into().unwrap()),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![
            (10, 103), // 100 + 1 + 2
            (20, 200), // base only
            (30, 303), // 300 + 3
        ]
    );

    Ok(())
}

// ===========================================================================
// Section 4: Merge operator + ReverseComparator
//
// These tests exercise merge-operand resolution with a genuinely
// non-lexicographic comparator across in-memtable, cross-flush, and
// post-compaction scenarios. Historically some of these cases reproduced
// the Run::push() sorting bug fixed in Section 2.
// ===========================================================================

#[test]
fn reverse_comparator_merge_operands_in_memtable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("a", 5_i64.to_le_bytes(), 1);
    tree.insert("b", 20_i64.to_le_bytes(), 2);
    tree.merge("b", 3_i64.to_le_bytes(), 3);
    tree.insert("c", 30_i64.to_le_bytes(), 4);

    // Point reads resolve merge operands correctly
    assert_eq!(Some(15), get_counter(&tree, b"a", 5));
    assert_eq!(Some(23), get_counter(&tree, b"b", 5));
    assert_eq!(Some(30), get_counter(&tree, b"c", 5));

    // Iteration in reverse order: c, b, a — each with resolved values
    let items: Vec<(String, i64)> = tree
        .iter(5, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                i64::from_le_bytes((*v).try_into().unwrap()),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![("c".into(), 30), ("b".into(), 23), ("a".into(), 15),]
    );

    Ok(())
}

#[test]
fn reverse_comparator_merge_after_single_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    tree.insert("x", 100_i64.to_le_bytes(), seqno.next());
    tree.merge("x", 10_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Single SST — no inter-SST merge needed, works correctly
    assert_eq!(Some(110), get_counter(&tree, b"x", SeqNo::MAX));

    Ok(())
}

#[test]
fn reverse_comparator_merge_across_flush_boundary() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    // Base + first operand in SST
    tree.insert("counter", 100_i64.to_le_bytes(), seqno.next());
    tree.merge("counter", 10_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // More operands in active memtable
    tree.merge("counter", 20_i64.to_le_bytes(), seqno.next());
    tree.merge("counter", 30_i64.to_le_bytes(), seqno.next());

    // Should merge across memtable + SST boundary
    assert_eq!(Some(160), get_counter(&tree, b"counter", SeqNo::MAX));

    Ok(())
}

#[test]
fn reverse_comparator_merge_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    // Batch 1: base values
    tree.insert("a", 10_i64.to_le_bytes(), seqno.next());
    tree.insert("b", 20_i64.to_le_bytes(), seqno.next());
    tree.insert("c", 30_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Batch 2: merge operands
    tree.merge("a", 5_i64.to_le_bytes(), seqno.next());
    tree.merge("b", 7_i64.to_le_bytes(), seqno.next());
    tree.merge("c", 3_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    // Compact: merge operands should be resolved during compaction
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // Point reads after compaction
    assert_eq!(Some(15), get_counter(&tree, b"a", SeqNo::MAX));
    assert_eq!(Some(27), get_counter(&tree, b"b", SeqNo::MAX));
    assert_eq!(Some(33), get_counter(&tree, b"c", SeqNo::MAX));

    // Iteration order: reverse (c, b, a)
    let items: Vec<(String, i64)> = tree
        .iter(SeqNo::MAX, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                i64::from_le_bytes((*v).try_into().unwrap()),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![("c".into(), 33), ("b".into(), 27), ("a".into(), 15),]
    );

    Ok(())
}

#[test]
#[ignore = "range bounds interpretation for reverse comparator (#116)"]
fn reverse_comparator_merge_range_scan_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(CounterMerge)))
        .open()?;

    // Insert base values for a..e
    for c in b'a'..=b'e' {
        tree.insert(&[c], 10_i64.to_le_bytes(), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // Merge operands for b and d
    tree.merge("b", 5_i64.to_le_bytes(), seqno.next());
    tree.merge("d", 8_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // Reverse order: e, d, c, b, a — range "d"..="b" yields d, c, b
    let items: Vec<(String, i64)> = tree
        .range("d"..="b", SeqNo::MAX, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                i64::from_le_bytes((*v).try_into().unwrap()),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![
            ("d".into(), 18), // 10 + 8
            ("c".into(), 10), // no merge operand
            ("b".into(), 15), // 10 + 5
        ]
    );

    Ok(())
}
