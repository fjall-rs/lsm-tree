use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_multi_table_ids() -> lsm_tree::Result<()> {
    let folder0 = tempfile::tempdir()?;
    let folder1 = tempfile::tempdir()?;

    let tree0 = Config::new(&folder0, SequenceNumberCounter::default()).open()?;
    assert_eq!(tree0.id(), 0);

    assert_eq!(0, tree0.next_table_id());

    tree0.insert("a", "a", 0);
    tree0.flush_active_memtable(0)?;

    assert_eq!(1, tree0.next_table_id());

    assert_eq!(
        0,
        tree0
            .current_version()
            .level(0)
            .expect("level should exist")
            .first()
            .expect("run should exist")
            .first()
            .expect("table should exist")
            .metadata
            .id
    );

    let tree1 = Config::new(&folder1, SequenceNumberCounter::default()).open()?;
    assert_eq!(tree1.id(), 1);

    assert_eq!(0, tree1.next_table_id());

    tree1.insert("a", "a", 0);
    tree1.flush_active_memtable(0)?;

    assert_eq!(1, tree1.next_table_id());

    assert_eq!(
        0,
        tree1
            .current_version()
            .level(0)
            .expect("level should exist")
            .first()
            .expect("run should exist")
            .first()
            .expect("table should exist")
            .metadata
            .id
    );

    Ok(())
}
