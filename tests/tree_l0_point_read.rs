use lsm_tree::{get_tmp_folder, AbstractTree, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_l0_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 1);
    tree.insert("b", "b", 1);
    tree.insert("c", "c", 1);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "A", 2);
    tree.insert("b", "B", 2);
    tree.insert("c", "C", 2);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("d", "d", 3);
    tree.insert("e", "e", 3);
    tree.insert("f", "f", 3);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("g", "g", 3);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(b"A", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert_eq!(b"B", &*tree.get("b", SeqNo::MAX)?.unwrap());
    assert_eq!(b"C", &*tree.get("c", SeqNo::MAX)?.unwrap());
    assert_eq!(b"d", &*tree.get("d", SeqNo::MAX)?.unwrap());
    assert_eq!(b"e", &*tree.get("e", SeqNo::MAX)?.unwrap());
    assert_eq!(b"f", &*tree.get("f", SeqNo::MAX)?.unwrap());
    assert_eq!(b"g", &*tree.get("g", SeqNo::MAX)?.unwrap());

    Ok(())
}
