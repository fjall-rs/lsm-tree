use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
#[ignore]
fn blob_gc_seqno_watermark() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .compression(lsm_tree::CompressionType::None)
        .open_as_blob_tree()?;
    let seqno = SequenceNumberCounter::default();

    tree.insert("a", "neptune".repeat(10_000), seqno.next());
    let snapshot = tree.snapshot(seqno.get());
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"neptune".repeat(10_000));
    assert_eq!(&*tree.get("a", None)?.unwrap(), b"neptune".repeat(10_000));

    tree.insert("a", "neptune2".repeat(10_000), seqno.next());
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"neptune".repeat(10_000));
    assert_eq!(&*tree.get("a", None)?.unwrap(), b"neptune2".repeat(10_000));

    tree.insert("a", "neptune3".repeat(10_000), seqno.next());
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"neptune".repeat(10_000));
    assert_eq!(&*tree.get("a", None)?.unwrap(), b"neptune3".repeat(10_000));

    tree.flush_active_memtable(0)?;
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"neptune".repeat(10_000));
    assert_eq!(&*tree.get("a", None)?.unwrap(), b"neptune3".repeat(10_000));

    let report = tree.gc_scan_stats(seqno.get() + 1, 0)?;
    assert_eq!(2, report.stale_blobs);

    let strategy = value_log::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, 0)?;

    // IMPORTANT: We cannot drop any blobs yet
    // because we the watermark is too low
    //
    // This would previously fail
    let report = tree.gc_scan_stats(seqno.get() + 1, 0)?;
    assert_eq!(2, report.stale_blobs);

    assert_eq!(&*snapshot.get("a")?.unwrap(), b"neptune".repeat(10_000));
    assert_eq!(&*tree.get("a", None)?.unwrap(), b"neptune3".repeat(10_000));

    Ok(())
}
