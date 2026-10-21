use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn leveled_trivial_move_into_l1() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    tree.insert("a", "a", 0);
    version_set_provider.flush_active_memtable(&tree, 1, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 2, 0)?;
    assert_eq!(1, tree.level_table_count(6).unwrap_or_default());

    tree.insert("b", "b", 3);
    version_set_provider.flush_active_memtable(&tree, 4, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 5, 0)?;
    assert_eq!(2, tree.level_table_count(6).unwrap_or_default());

    tree.insert("c", "c", 6);
    version_set_provider.flush_active_memtable(&tree, 7, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 8, 0)?;
    assert_eq!(3, tree.level_table_count(6).unwrap_or_default());

    tree.insert("d", "d", 9);
    version_set_provider.flush_active_memtable(&tree, 10, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 11, 0)?;
    assert_eq!(0, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(4, tree.level_table_count(6).unwrap_or_default());

    // To keep runs minimal, we trivial move into L1
    tree.insert("e", "e", 12);
    version_set_provider.flush_active_memtable(&tree, 13, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 14, 0)?;
    assert_eq!(0, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(5, tree.level_table_count(6).unwrap_or_default());

    // To keep runs minimal, we trivial move into L1
    tree.insert("f", "f", 15);
    version_set_provider.flush_active_memtable(&tree, 16, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 17, 0)?;
    assert_eq!(0, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(6, tree.level_table_count(6).unwrap_or_default());

    // Should not trivial move because overlap
    tree.insert("d", "d", 18);
    version_set_provider.flush_active_memtable(&tree, 19, 0)?;
    version_set_provider.compact(&tree, compaction.clone(), 20, 0)?;
    assert_eq!(1, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(6, tree.level_table_count(6).unwrap_or_default());

    Ok(())
}
