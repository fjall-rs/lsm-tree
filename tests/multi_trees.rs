use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_multi_segment_ids() -> lsm_tree::Result<()> {
    let folder0 = tempfile::tempdir()?;
    let folder1 = tempfile::tempdir()?;

    let tree0 = Config::new(&folder0).open()?;
    assert_eq!(tree0.id, 0);

    assert_eq!(
        0,
        tree0
            .0
            .segment_id_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    tree0.insert("a", "a", 0);
    tree0.flush_active_memtable(0)?;

    assert_eq!(
        1,
        tree0
            .0
            .segment_id_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    assert_eq!(
        0,
        tree0
            .manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .level(0)
            .expect("level should exist")
            .first()
            .expect("run should exist")
            .first()
            .expect("segment should exist")
            .metadata
            .id
    );

    let tree1 = Config::new(&folder1).open()?;
    assert_eq!(tree1.id, 1);

    assert_eq!(
        0,
        tree1
            .0
            .segment_id_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    tree1.insert("a", "a", 0);
    tree1.flush_active_memtable(0)?;

    assert_eq!(
        1,
        tree1
            .0
            .segment_id_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    assert_eq!(
        0,
        tree1
            .manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .level(0)
            .expect("level should exist")
            .first()
            .expect("run should exist")
            .first()
            .expect("segment should exist")
            .metadata
            .id
    );

    Ok(())
}
