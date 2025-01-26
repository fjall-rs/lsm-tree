use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_tree_tombstone() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open_as_blob_tree()?;

    let big_value = b"neptune!".repeat(128_000);

    tree.insert("a", &big_value, 0);
    tree.insert("b", &big_value, 0);
    tree.insert("c", &big_value, 0);
    assert_eq!(3, tree.len(None, None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(3, tree.len(None, None)?);

    tree.remove("b", 1);
    assert_eq!(2, tree.len(None, None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.len(None, None)?);

    assert_eq!(&*tree.get("a", None)?.unwrap(), big_value);
    assert!(tree.get("b", None)?.is_none());
    assert_eq!(&*tree.get("c", None)?.unwrap(), big_value);

    tree.gc_scan_stats(2, 0)?;

    let strategy = value_log::StaleThresholdStrategy::new(0.01);
    tree.apply_gc_strategy(&strategy, 2)?;
    assert_eq!(2, tree.len(None, None)?);

    assert_eq!(&*tree.get("a", None)?.unwrap(), big_value);
    assert!(tree.get("b", None)?.is_none());
    assert_eq!(&*tree.get("c", None)?.unwrap(), big_value);

    Ok(())
}
