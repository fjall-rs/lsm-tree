use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_range_count() -> lsm_tree::Result<()> {
    use std::ops::Bound::{self, Excluded, Unbounded};

    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 0);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 1);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 2);

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 3);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 4);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 5);

    assert_eq!(2, tree.range("a"..="f").count());
    assert_eq!(2, tree.range("f"..="g").count());

    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .count()
    );

    tree.flush_active_memtable()?;

    assert_eq!(2, tree.range("a"..="f").count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .count()
    );

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 6);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 7);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 8);

    assert_eq!(2, tree.range("a"..="f").count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .count()
    );

    Ok(())
}

#[test]
fn blob_tree_range_count() -> lsm_tree::Result<()> {
    use std::ops::Bound::{self, Excluded, Unbounded};

    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open_as_blob_tree()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 0);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 1);
    tree.insert("g".as_bytes(), b"neptune!".repeat(128_000), 2);

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 3);
    tree.insert("f".as_bytes(), b"neptune!".repeat(128_000), 4);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 5);

    assert_eq!(2, tree.range("a"..="f").count());
    assert_eq!(2, tree.range("f"..="g").count());

    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .count()
    );

    tree.flush_active_memtable()?;

    assert_eq!(2, tree.range("a"..="f").count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .count()
    );

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 6);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 7);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 8);

    assert_eq!(2, tree.range("a"..="f").count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .count()
    );

    Ok(())
}
