use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_builds_filters() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .expect_point_read_hits(false)
        .open()?;

    tree.insert("a", "a", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert!(tree.filter_size() > 0);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1,
        0,
    )?;

    assert!(tree.filter_size() > 0);

    Ok(())
}

#[test]
fn tree_expect_point_read_hits_last_level() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .expect_point_read_hits(true)
        .open()?;

    tree.insert("a", "a", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert!(tree.filter_size() > 0);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1,
        0,
    )?;

    assert!(tree.filter_size() == 0);

    Ok(())
}
