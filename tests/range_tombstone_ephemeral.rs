// Tests for per-source RT visibility when ephemeral memtable index_seqno
// differs from the outer scan seqno (issue #33).
//
// The ephemeral memtable is an overlay whose KV stream is gated at its own
// `index_seqno`.  Range tombstones from the ephemeral source must use that
// same cutoff — not the outer scan seqno — so that:
//   • Over-suppress is prevented (RT visible at outer_seqno but not at
//     eph_seqno must NOT suppress keys from other sources).
//   • Leak is prevented (RT visible at eph_seqno but not at outer_seqno
//     must still suppress keys that entered the merged stream through the
//     ephemeral source).

// Guard: trait import required for .key() method on iterator items (IterGuard trait)
use lsm_tree::{
    get_tmp_folder, AbstractTree, AnyTree, Config, Guard, Memtable, SequenceNumberCounter, UserKey,
};
use std::sync::Arc;
use test_log::test;

fn open_tree(path: &std::path::Path) -> AnyTree {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("should open")
}

/// Arbitrary ID for ephemeral memtables in tests (distinct from tree-managed IDs).
const EPHEMERAL_MT_ID: lsm_tree::MemtableId = 999;

/// Build an ephemeral memtable with the given KVs and range tombstones.
fn build_ephemeral(kvs: &[(&[u8], &[u8], u64)], rts: &[(&[u8], &[u8], u64)]) -> Arc<Memtable> {
    let mt = Arc::new(Memtable::new(
        EPHEMERAL_MT_ID,
        std::sync::Arc::new(lsm_tree::DefaultUserComparator),
    ));
    for &(key, val, seqno) in kvs {
        mt.insert(lsm_tree::InternalValue::from_components(
            key,
            val,
            seqno,
            lsm_tree::ValueType::Value,
        ));
    }
    for &(start, end, seqno) in rts {
        assert!(
            mt.insert_range_tombstone(UserKey::from(start), UserKey::from(end), seqno) > 0,
            "insert_range_tombstone returned 0 (rejected)",
        );
    }
    mt
}

/// Collect keys from a forward iterator.
/// Returns `Vec<Vec<u8>>` which compares correctly with `vec![b"a", b"b"]`
/// via Rust's `PartialEq` blanket impl for `Vec<T>` where `T: PartialEq<U>`.
fn collect_keys(
    tree: &AnyTree,
    seqno: u64,
    eph: Option<(Arc<Memtable>, u64)>,
) -> lsm_tree::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for item in tree.iter(seqno, eph) {
        keys.push(item.key()?.to_vec());
    }
    Ok(keys)
}

/// Collect keys from a reverse iterator.
fn collect_keys_rev(
    tree: &AnyTree,
    seqno: u64,
    eph: Option<(Arc<Memtable>, u64)>,
) -> lsm_tree::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for item in tree.iter(seqno, eph).rev() {
        keys.push(item.key()?.to_vec());
    }
    Ok(keys)
}

/// Collect keys from a range iterator.
fn collect_range_keys<R>(
    tree: &AnyTree,
    range: R,
    seqno: u64,
    eph: Option<(Arc<Memtable>, u64)>,
) -> lsm_tree::Result<Vec<Vec<u8>>>
where
    R: std::ops::RangeBounds<&'static str>,
{
    let mut keys = Vec::new();
    for item in tree.range(range, seqno, eph) {
        keys.push(item.key()?.to_vec());
    }
    Ok(keys)
}

/// Collect keys from a prefix iterator.
fn collect_prefix_keys(
    tree: &AnyTree,
    prefix: &str,
    seqno: u64,
    eph: Option<(Arc<Memtable>, u64)>,
) -> lsm_tree::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for item in tree.prefix(prefix, seqno, eph) {
        keys.push(item.key()?.to_vec());
    }
    Ok(keys)
}

// ─────────────────────────────────────────────────────────────────────────
// Over-suppress: eph_seqno < outer_seqno
// An ephemeral RT at seqno X where eph_seqno <= X < outer_seqno would be
// visible at outer_seqno but NOT at eph_seqno.  Without per-source cutoff,
// it would incorrectly suppress base-tree keys.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn ephemeral_rt_not_visible_at_eph_seqno_does_not_suppress_base_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Base tree: keys a..d at seqno 1
    tree.insert("a", "v", 1);
    tree.insert("b", "v", 1);
    tree.insert("c", "v", 1);
    tree.insert("d", "v", 1);

    // Ephemeral: RT [b, d) at seqno 15.
    // eph_seqno = 10 → RT NOT visible (15 >= 10).
    // outer_seqno = 20 → RT IS visible (15 < 20).
    // Without fix: RT would suppress b,c from base tree.
    // With fix: RT uses eph_seqno=10 as cutoff → invisible → no suppression.
    let eph = build_ephemeral(&[], &[(b"b", b"d", 15)]);

    let keys = collect_keys(&tree, 20, Some((eph.clone(), 10)))?;
    assert_eq!(keys, vec![b"a", b"b", b"c", b"d"]);

    // Same check in reverse
    let keys_rev = collect_keys_rev(&tree, 20, Some((eph, 10)))?;
    assert_eq!(keys_rev, vec![b"d", b"c", b"b", b"a"]);

    Ok(())
}

#[test]
fn ephemeral_rt_not_visible_at_eph_seqno_range_query() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "v", 1);
    tree.insert("b", "v", 1);
    tree.insert("c", "v", 1);

    // Ephemeral RT [a, d) at seqno 15, eph_seqno=10, outer_seqno=20.
    let eph = build_ephemeral(&[], &[(b"a", b"d", 15)]);

    let keys = collect_range_keys(&tree, "a"..="c", 20, Some((eph, 10)))?;
    assert_eq!(keys, vec![b"a", b"b", b"c"]);

    Ok(())
}

#[test]
fn ephemeral_rt_not_visible_at_eph_seqno_prefix_query() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("pre:a", "v", 1);
    tree.insert("pre:b", "v", 1);
    tree.insert("pre:c", "v", 1);

    // Ephemeral RT [pre:a, pre:d) at seqno 15, eph_seqno=10, outer_seqno=20.
    let eph = build_ephemeral(&[], &[(b"pre:a", b"pre:d", 15)]);

    let keys = collect_prefix_keys(&tree, "pre:", 20, Some((eph, 10)))?;
    assert_eq!(keys, vec![b"pre:a", b"pre:b", b"pre:c"]);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Leak: eph_seqno > outer_seqno
// An ephemeral RT at seqno X where outer_seqno <= X < eph_seqno is visible
// at eph_seqno but NOT at outer_seqno.  The RT should still suppress
// ephemeral KVs that entered the merged stream.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn ephemeral_rt_visible_at_eph_seqno_suppresses_ephemeral_kvs() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Base tree: key "a" at seqno 1
    tree.insert("a", "base", 1);

    // Ephemeral: KVs b,c at seqno 5; RT [b, d) at seqno 8.
    // eph_seqno = 10 → RT visible (8 < 10). KVs visible (5 < 10).
    // outer_seqno = 6 → RT NOT visible with outer cutoff (8 >= 6).
    // Without fix: RT uses outer_seqno=6 as cutoff → invisible → b,c leak.
    // With fix: RT uses eph_seqno=10 as cutoff → visible → b,c suppressed.
    let eph = build_ephemeral(&[(b"b", b"vb", 5), (b"c", b"vc", 5)], &[(b"b", b"d", 8)]);

    let keys = collect_keys(&tree, 6, Some((eph.clone(), 10)))?;
    // "a" from base (seqno 1 < outer_seqno 6), b and c suppressed by eph RT
    assert_eq!(keys, vec![b"a"]);

    // Reverse
    let keys_rev = collect_keys_rev(&tree, 6, Some((eph, 10)))?;
    assert_eq!(keys_rev, vec![b"a"]);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Normal case: eph_seqno == outer_seqno (no divergence)
// Sanity check that the per-source cutoff doesn't break the common case.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn ephemeral_rt_same_seqno_still_suppresses() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "v", 1);
    tree.insert("b", "v", 1);
    tree.insert("c", "v", 1);

    // Ephemeral RT [a, c) at seqno 5, both seqnos = 10.
    let eph = build_ephemeral(&[], &[(b"a", b"c", 5)]);

    let keys = collect_keys(&tree, 10, Some((eph.clone(), 10)))?;
    assert_eq!(keys, vec![b"c"]);

    let keys_rev = collect_keys_rev(&tree, 10, Some((eph, 10)))?;
    assert_eq!(keys_rev, vec![b"c"]);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Duplicate RT from two sources with different cutoffs
// The same RT may exist in both a persisted SST and the ephemeral memtable.
// Dedup must preserve the higher cutoff so the RT remains visible when at
// least one source's snapshot includes it.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn duplicate_rt_from_two_sources_keeps_max_cutoff() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Base tree: keys a,b,c at seqno 1; RT [a, d) at seqno 5 in memtable
    tree.insert("a", "v", 1);
    tree.insert("b", "v", 1);
    tree.insert("c", "v", 1);
    tree.remove_range("a", "d", 5);

    // Flush to SST so the RT is persisted (cutoff will be outer_seqno=4)
    tree.flush_active_memtable(0)?;

    // Ephemeral: same RT [a, d) at seqno 5 (cutoff will be eph_seqno=10)
    // outer_seqno = 4 → persisted RT NOT visible (5 >= 4)
    // eph_seqno = 10 → ephemeral RT IS visible (5 < 10)
    // If dedup drops the ephemeral copy, the RT becomes invisible and a,b,c leak.
    let eph = build_ephemeral(&[], &[(b"a", b"d", 5)]);

    let keys = collect_keys(&tree, 4, Some((eph.clone(), 10)))?;
    // a,b,c must be suppressed — the ephemeral copy's cutoff=10 makes the RT visible
    assert_eq!(keys, Vec::<Vec<u8>>::new());

    // Reverse
    let keys_rev = collect_keys_rev(&tree, 4, Some((eph, 10)))?;
    assert_eq!(keys_rev, Vec::<Vec<u8>>::new());

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Base-tree RT should not be affected by ephemeral seqno
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn base_rt_uses_outer_seqno_not_ephemeral() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "v", 1);
    tree.insert("b", "v", 1);
    tree.insert("c", "v", 1);

    // Base-tree RT [a, c) at seqno 5
    tree.remove_range("a", "c", 5);

    // Ephemeral: just KV "x" — no RTs.
    // eph_seqno = 3, outer_seqno = 10.
    // Base RT should use outer_seqno=10 → visible (5 < 10) → suppresses a,b.
    let eph = build_ephemeral(&[(b"x", b"vx", 1)], &[]);

    let keys = collect_keys(&tree, 10, Some((eph, 3)))?;
    assert_eq!(keys, vec![b"c", b"x"]);

    Ok(())
}
