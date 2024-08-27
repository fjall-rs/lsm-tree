use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

#[test_log::test]
fn blob_gc_1() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open_as_blob_tree()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a", "neptune".repeat(10_000), seqno.next());
    tree.insert("b", "neptune".repeat(10_000), seqno.next());
    tree.insert("c", "neptune".repeat(10_000), seqno.next());

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blobs.segment_count());

    tree.gc_scan_stats(seqno.get())?;

    assert_eq!(1.0, tree.blobs.space_amp());

    tree.insert("a", "a", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(1.5, tree.blobs.space_amp());

    tree.insert("b", "b", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(3.0, tree.blobs.space_amp());

    // NOTE: Everything is stale
    tree.insert("c", "c", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(0.0, tree.blobs.space_amp());

    tree.gc_drop_stale()?;

    assert_eq!(&*tree.get("a")?.unwrap(), b"a");
    assert_eq!(&*tree.get("b")?.unwrap(), b"b");
    assert_eq!(&*tree.get("c")?.unwrap(), b"c");
    assert_eq!(0, tree.blobs.segment_count());
    assert_eq!(0.0, tree.blobs.space_amp());

    Ok(())
}

#[test_log::test]
fn blob_gc_2() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open_as_blob_tree()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a", "neptune".repeat(10_000), seqno.next());
    tree.insert("b", "neptune".repeat(10_000), seqno.next());
    tree.insert("c", "neptune".repeat(10_000), seqno.next());

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blobs.segment_count());

    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(1.0, tree.blobs.space_amp());

    tree.insert("a", "a", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(1.5, tree.blobs.space_amp());

    tree.insert("b", "b", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(3.0, tree.blobs.space_amp());

    let strategy = value_log::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, seqno.next())?;

    assert_eq!(&*tree.get("a")?.unwrap(), b"a");
    assert_eq!(&*tree.get("b")?.unwrap(), b"b");
    assert_eq!(
        &*tree.get("c")?.unwrap(),
        "neptune".repeat(10_000).as_bytes()
    );
    assert_eq!(1, tree.blobs.segment_count());
    assert_eq!(1.0, tree.blobs.space_amp());

    tree.insert("c", "c", seqno.next());

    tree.gc_scan_stats(seqno.get())?;

    let strategy = value_log::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, seqno.next())?;
    assert_eq!(0, tree.blobs.segment_count());

    Ok(())
}

#[test_log::test]
fn blob_gc_3() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open_as_blob_tree()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a", "neptune".repeat(10_000), seqno.next());
    tree.insert("b", "neptune".repeat(10_000), seqno.next());
    tree.insert("c", "neptune".repeat(10_000), seqno.next());

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blobs.segment_count());

    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(1.0, tree.blobs.space_amp());

    tree.remove("a", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(1.5, tree.blobs.space_amp());

    tree.remove("b", seqno.next());
    tree.gc_scan_stats(seqno.get())?;
    assert_eq!(3.0, tree.blobs.space_amp());

    let strategy = value_log::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, seqno.next())?;

    assert!(tree.get("a")?.is_none());
    assert!(tree.get("b")?.is_none());
    assert_eq!(
        &*tree.get("c")?.unwrap(),
        "neptune".repeat(10_000).as_bytes()
    );
    assert_eq!(1, tree.blobs.segment_count());
    assert_eq!(1.0, tree.blobs.space_amp());

    tree.remove("c", seqno.next());
    assert!(tree.get("c")?.is_none());

    tree.gc_scan_stats(seqno.get())?;

    let strategy = value_log::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, seqno.next())?;
    assert_eq!(0, tree.blobs.segment_count());

    Ok(())
}
