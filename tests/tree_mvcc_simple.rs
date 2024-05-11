use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_read_mvcc() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    tree.insert("a", "a0", 0);
    tree.insert("a", "a1", 1);
    tree.insert("b", "b0", 0);
    tree.insert("b", "b1", 1);

    tree.flush_active_memtable()?;

    tree.insert("b", "b2", 2);
    tree.insert("b", "b3", 3);
    tree.insert("c", "c4", 4);

    tree.flush_active_memtable()?;

    tree.insert("a", "a5", 5);

    assert_eq!(&*tree.get("a")?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b")?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c")?.unwrap(), b"c4");

    let snapshot = tree.snapshot(1);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a0");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b0");
    assert!(snapshot.get("c")?.is_none());

    let snapshot = tree.snapshot(2);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a1");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b1");
    assert!(snapshot.get("c")?.is_none());

    let snapshot = tree.snapshot(3);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a1");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b2");
    assert!(snapshot.get("c")?.is_none());

    let snapshot = tree.snapshot(4);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a1");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b3");
    assert!(snapshot.get("c")?.is_none());

    let snapshot = tree.snapshot(5);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a1");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b3");
    assert_eq!(&*snapshot.get("c")?.unwrap(), b"c4");

    let snapshot = tree.snapshot(6);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a5");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b3");
    assert_eq!(&*snapshot.get("c")?.unwrap(), b"c4");

    let snapshot = tree.snapshot(100);
    assert_eq!(&*snapshot.get("a")?.unwrap(), b"a5");
    assert_eq!(&*snapshot.get("b")?.unwrap(), b"b3");
    assert_eq!(&*snapshot.get("c")?.unwrap(), b"c4");

    let reader = tree.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a5");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b3");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"c4");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(1);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a0");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b0");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(2);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a1");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b1");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(3);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a1");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b2");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(4);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a1");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b3");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(5);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a1");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b3");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"c4");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(6);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a5");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b3");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"c4");
    assert!(iter.next().is_none());

    let snapshot = tree.snapshot(100);
    let reader = snapshot.iter();
    let mut iter = reader.into_iter();

    assert_eq!(&*iter.next().unwrap().unwrap().1, b"a5");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"b3");
    assert_eq!(&*iter.next().unwrap().unwrap().1, b"c4");
    assert!(iter.next().is_none());

    Ok(())
}
