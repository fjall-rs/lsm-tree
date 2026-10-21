use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_highest_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    assert_eq!(tree.get_highest_seqno(), None);
    assert_eq!(tree.get_highest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("a", "a0", 0);
    assert_eq!(tree.get_highest_seqno(), Some(0));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(0));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("a", "a1", 1);
    assert_eq!(tree.get_highest_seqno(), Some(1));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(1));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("b", "b0", 2);
    assert_eq!(tree.get_highest_seqno(), Some(2));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(2));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("b", "b1", 3);
    assert_eq!(tree.get_highest_seqno(), Some(3));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(3));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    version_set_provider.flush_active_memtable(&tree, 4, 0)?;

    assert_eq!(tree.get_highest_seqno(), Some(3));
    assert_eq!(tree.get_highest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    tree.insert("a", "a0", 5);
    assert_eq!(tree.get_highest_seqno(), Some(5));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(5));
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    version_set_provider.rotate_active_memtable();

    assert_eq!(tree.get_highest_seqno(), Some(5));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(5));
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    let flush_result = tree.new_flush(0)?.expect("should flush to table files");
    version_set_provider.register_flush(flush_result, 6);

    assert_eq!(tree.get_highest_seqno(), Some(5));
    assert_eq!(tree.get_highest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), Some(5));

    Ok(())
}
