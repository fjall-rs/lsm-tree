use lsm_tree::{AbstractTree, Guard, SeqNo};
use test_log::test;

#[test]
fn tree_l0_range_blob() -> lsm_tree::Result<()> {
    let folder: tempfile::TempDir = tempfile::tempdir()?;
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

    tree.insert("g", "g".repeat(10_000), 3);
    tree.flush_active_memtable(0)?;

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
