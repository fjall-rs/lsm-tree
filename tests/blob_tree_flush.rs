use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
#[ignore]
fn blob_gc_flush_tombstone() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open_as_blob_tree()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a", "neptune".repeat(10_000), seqno.next());
    tree.insert("b", "neptune".repeat(10_000), seqno.next());
    tree.flush_active_memtable(0)?;

    tree.remove("b", seqno.next());

    tree.gc_scan_stats(seqno.get(), /* simulate some time has passed */ 1_000)?;
    assert_eq!(2.0, tree.space_amp());

    let strategy = lsm_tree::gc::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, seqno.next())?;
    assert_eq!(1, tree.blob_file_count());

    tree.gc_scan_stats(seqno.get(), 1_000)?;
    assert_eq!(1.0, tree.space_amp());

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blob_file_count());

    tree.gc_scan_stats(seqno.get(), 1_000)?;
    assert_eq!(1.0, tree.space_amp());

    Ok(())
}
