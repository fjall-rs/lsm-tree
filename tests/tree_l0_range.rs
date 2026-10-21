use lsm_tree::{get_tmp_folder, AbstractTree, Guard, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_l0_range_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 2);
    tree.insert("b", "b", 2);
    tree.insert("c", "c", 2);

    version_set_provider.flush_active_memtable(&tree, 3, 0)?;

    tree.insert("a", "A", 4);
    tree.insert("b", "B", 4);
    tree.insert("c", "C", 4);

    version_set_provider.flush_active_memtable(&tree, 5, 0)?;

    tree.insert("d", "d", 6);
    tree.insert("e", "e", 6);
    tree.insert("f", "f", 6);

    version_set_provider.flush_active_memtable(&tree, 7, 0)?;

    tree.insert("g", "g".repeat(10_000), 8);

    version_set_provider.flush_active_memtable(&tree, 9, 0)?;

    let mut range = tree.range("c"..="e", SeqNo::MAX, None);
    assert_eq!(b"C", &*range.next().unwrap().value()?);
    assert_eq!(b"d", &*range.next().unwrap().value()?);
    assert_eq!(b"e", &*range.next().unwrap().value()?);
    assert!(range.next().is_none());

    let mut range = tree.range("f"..="g", SeqNo::MAX, None).rev();
    assert_eq!(b"g".repeat(10_000), &*range.next().unwrap().value()?);
    assert_eq!(b"f", &*range.next().unwrap().value()?);
    assert!(range.next().is_none());

    Ok(())
}
