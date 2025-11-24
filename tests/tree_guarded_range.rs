use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_guarded_range() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;

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
fn blob_tree_guarded_range() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, SequenceNumberCounter::default())
        .with_kv_separation(Some(Default::default()))
        .open()?;

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
