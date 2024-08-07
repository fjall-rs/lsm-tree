use lsm_tree::{AbstractTree, Config};

#[test_log::test]
fn tree_highest_seqno() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).open()?;
    assert_eq!(tree.get_highest_seqno(), None);
    assert_eq!(tree.get_higest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("a", "a0", 0);
    assert_eq!(tree.get_highest_seqno(), Some(0));
    assert_eq!(tree.get_higest_memtable_seqno(), Some(0));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("a", "a1", 1);
    assert_eq!(tree.get_highest_seqno(), Some(1));
    assert_eq!(tree.get_higest_memtable_seqno(), Some(1));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("b", "b0", 2);
    assert_eq!(tree.get_highest_seqno(), Some(2));
    assert_eq!(tree.get_higest_memtable_seqno(), Some(2));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("b", "b1", 3);
    assert_eq!(tree.get_highest_seqno(), Some(3));
    assert_eq!(tree.get_higest_memtable_seqno(), Some(3));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.flush_active_memtable()?;
    assert_eq!(tree.get_highest_seqno(), Some(3));
    assert_eq!(tree.get_higest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    tree.insert("a", "a0", 4);
    assert_eq!(tree.get_highest_seqno(), Some(4));
    assert_eq!(tree.get_higest_memtable_seqno(), Some(4));
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    Ok(())
}
