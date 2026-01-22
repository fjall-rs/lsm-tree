use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn leveled_trivial_move_into_l1() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(1, tree.level_table_count(6).unwrap_or_default());

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(2, tree.level_table_count(6).unwrap_or_default());

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(3, tree.level_table_count(6).unwrap_or_default());

    tree.insert("d", "d", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(0, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(4, tree.level_table_count(6).unwrap_or_default());

    // To keep runs minimal, we trivial move into L1
    tree.insert("e", "e", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(0, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(5, tree.level_table_count(6).unwrap_or_default());

    // To keep runs minimal, we trivial move into L1
    tree.insert("f", "f", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(0, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(6, tree.level_table_count(6).unwrap_or_default());
    // Should not trivial move because overlap
    tree.insert("d", "d", 0);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(1, tree.level_table_count(0).unwrap_or_default());
    assert_eq!(6, tree.level_table_count(6).unwrap_or_default());

    Ok(())
}
