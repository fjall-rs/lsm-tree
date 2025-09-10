use lsm_tree::{AbstractTree, Config, SeqNo};
use std::time::Duration;
use test_log::test;

#[test]
#[ignore = "restore"]
fn blob_drop_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .compression(lsm_tree::CompressionType::None)
        .open_as_blob_tree()?;

    tree.insert("a", "neptune".repeat(10_000), 0);
    let (id, memtable) = tree.rotate_memtable().unwrap();

    let segment = tree.flush_memtable(id, &memtable, 0).unwrap().unwrap();

    // NOTE: Segment is now in-flight

    let gc_report = std::thread::spawn({
        let tree = tree.clone();

        move || {
            let report = tree.gc_scan_stats(1, 0)?;
            Ok::<_, lsm_tree::Error>(report)
        }
    });

    std::thread::sleep(Duration::from_secs(1));

    let strategy = lsm_tree::gc::SpaceAmpStrategy::new(1.0);
    tree.apply_gc_strategy(&strategy, 0)?;

    tree.register_segments(&[segment], 0)?;

    assert_eq!(
        "neptune".repeat(10_000).as_bytes(),
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
    );

    let report = gc_report.join().unwrap()?;
    assert_eq!(0, report.stale_blobs);
    assert_eq!(1, report.total_blobs);

    Ok(())
}
