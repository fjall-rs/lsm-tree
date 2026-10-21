use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_read_mvcc() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a0", 0);
    tree.insert("a", "a1", 1);
    tree.insert("b", "b0", 0);
    tree.insert("b", "b1", 1);

    version_set_provider.flush_active_memtable(&tree, 2, 0)?;

    tree.insert("b", "b2", 3);
    tree.insert("b", "b3", 4);
    tree.insert("c", "c4", 5);

    version_set_provider.flush_active_memtable(&tree, 6, 0)?;

    tree.insert("a", "a5", 7);

    assert_eq!(&*tree.get("a", SeqNo::MAX)?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b", SeqNo::MAX)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", SeqNo::MAX)?.unwrap(), b"c4");

    assert_eq!(&*tree.get("a", 1)?.unwrap(), b"a0");
    assert_eq!(&*tree.get("b", 1)?.unwrap(), b"b0");
    assert!(tree.get("c", 1)?.is_none());

    assert_eq!(&*tree.get("a", 2)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", 2)?.unwrap(), b"b1");
    assert!(tree.get("c", 2)?.is_none());

    assert_eq!(&*tree.get("a", 4)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", 4)?.unwrap(), b"b2");
    assert!(tree.get("c", 4)?.is_none());

    assert_eq!(&*tree.get("a", 5)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", 5)?.unwrap(), b"b3");
    assert!(tree.get("c", 5)?.is_none());

    assert_eq!(&*tree.get("a", 6)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", 6)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", 6)?.unwrap(), b"c4");

    assert_eq!(&*tree.get("a", 8)?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b", 8)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", 8)?.unwrap(), b"c4");

    assert_eq!(&*tree.get("a", 100)?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b", 100)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", 100)?.unwrap(), b"c4");

    let mut iter = tree.iter(SeqNo::MAX, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a5");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(1, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a0");
    assert_eq!(&*iter.next().unwrap().value()?, b"b0");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(2, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b1");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(4, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b2");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(5, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(6, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(8, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a5");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(100, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a5");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    Ok(())
}
