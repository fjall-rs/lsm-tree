use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider};
use std::sync::Arc;

#[test]
fn weak_tombstone_counts_single_pair() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert(b"a", b"old", 1);
    tree.remove_weak(b"a", 2);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(tree.weak_tombstone_count(), 1);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 1);

    Ok(())
}

#[test]
fn weak_tombstone_counts_multiple_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert(b"a", b"old", 10);
    tree.remove_weak(b"a", 11);

    tree.remove_weak(b"b", 12);

    tree.insert(b"c", b"old", 13);
    tree.insert(b"c", b"new", 14);
    tree.remove_weak(b"c", 15);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(tree.weak_tombstone_count(), 3);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 2);

    Ok(())
}

#[test]
fn weak_tombstone_counts_multiple_weak() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert(b"a", b"old", 10);
    tree.remove_weak(b"a", 11);
    tree.remove_weak(b"a", 12);
    tree.remove_weak(b"a", 13);
    tree.remove_weak(b"a", 14);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(tree.weak_tombstone_count(), 4);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 1); // a:10 is paired with a:11

    Ok(())
}
