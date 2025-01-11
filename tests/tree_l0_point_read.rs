use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn tree_l0_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open_as_blob_tree()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 1);
    tree.insert("b", "b", 1);
    tree.insert("c", "c", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "A", 2);
    tree.insert("b", "B", 2);
    tree.insert("c", "C", 2);
    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 3);
    tree.insert("e", "e", 3);
    tree.insert("f", "f", 3);
    tree.flush_active_memtable(0)?;

    tree.insert("g", "g", 3);
    tree.flush_active_memtable(0)?;

    assert_eq!(b"A", &*tree.get("a", None)?.unwrap());
    assert_eq!(b"B", &*tree.get("b", None)?.unwrap());
    assert_eq!(b"C", &*tree.get("c", None)?.unwrap());
    assert_eq!(b"d", &*tree.get("d", None)?.unwrap());
    assert_eq!(b"e", &*tree.get("e", None)?.unwrap());
    assert_eq!(b"f", &*tree.get("f", None)?.unwrap());
    assert_eq!(b"g", &*tree.get("g", None)?.unwrap());

    Ok(())
}
