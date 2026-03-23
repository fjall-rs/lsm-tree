mod common;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

/// Regression test derived from proptest seed cc 90710f96...
///
/// The original proptest used an independent seqno counter (`let mut seqno = 1`)
/// that did not advance on flush/compact, violating the API contract which
/// requires data seqnos to come from the shared `SequenceNumberCounter` passed
/// to `Config::new`. With independent counters, the tree's internal SuperVersion
/// seqno advances faster than the data seqno, causing `get_version_for_snapshot`
/// to return a stale SuperVersion whose memtable misses recent inserts.
///
/// This test uses the shared counter (correct API usage) and verifies the
/// same operation pattern works correctly.
#[test]
fn point_read_after_compact_flush_returns_latest_value() -> lsm_tree::Result<()> {
    let tmpdir = lsm_tree::get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();
    let tree = Config::new(&tmpdir, seqno.clone(), visible_seqno.clone()).open()?;
    let k = vec![0u8];
    let v0 = vec![0u8; 8];
    let v1 = vec![1u8; 8];

    // No-op compact on empty tree
    let gc = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, gc)?;
    // No-op flush on empty memtable
    tree.flush_active_memtable(0)?;

    let s = seqno.next();
    tree.insert(&k, &v0, s);
    visible_seqno.fetch_max(s + 1);
    tree.flush_active_memtable(0)?;

    let s = seqno.next();
    tree.insert(&k, &v0, s);
    visible_seqno.fetch_max(s + 1);

    // Triple compact (first one moves L0→L6, next two re-compact L6)
    let gc = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, gc)?;
    tree.major_compact(common::COMPACTION_TARGET, gc)?;
    tree.major_compact(common::COMPACTION_TARGET, gc)?;
    // Flush the pending memtable entry
    tree.flush_active_memtable(0)?;

    // Insert+flush cycle
    for _ in 0..3 {
        let s = seqno.next();
        tree.insert(&k, &v0, s);
        visible_seqno.fetch_max(s + 1);
        tree.flush_active_memtable(0)?;
    }

    // Second major compact
    let gc = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, gc)?;

    // Insert + flush after compact (creates L0 table)
    let s = seqno.next();
    tree.insert(&k, &v0, s);
    visible_seqno.fetch_max(s + 1);
    tree.flush_active_memtable(0)?;

    // Two memtable inserts — last one has v1
    let s = seqno.next();
    tree.insert(&k, &v0, s);
    visible_seqno.fetch_max(s + 1);

    let s = seqno.next();
    tree.insert(&k, &v1, s);
    visible_seqno.fetch_max(s + 1);

    let read_seqno = visible_seqno.get();
    assert_eq!(
        tree.get(&k, read_seqno)?.as_ref().map(|v| v.to_vec()),
        Some(v1),
        "Point read at seqno={read_seqno} should return v1 (the latest insert)"
    );
    Ok(())
}
