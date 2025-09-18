use lsm_tree::{AbstractTree, Config, Guard, SeqNo};
use test_log::test;

#[test]
fn experimental_tree_guarded_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 0);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 1);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 2);

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 3);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 4);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 5);

    assert_eq!(
        2,
        tree.range("a"..="f", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );
    assert_eq!(
        2,
        tree.range("f"..="g", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );

    Ok(())
}

#[test]
#[ignore = "restore"]
fn experimental_blob_tree_guarded_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open_as_blob_tree()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 0);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 1);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 2);

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 3);
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), 4);
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), 5);

    assert_eq!(
        2,
        tree.range("a"..="f", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );
    assert_eq!(
        2,
        tree.range("f"..="g", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );

    Ok(())
}
