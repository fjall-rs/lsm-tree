use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_recover_table_counter() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let counter_expected = {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.next_table_id()
    };

    {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert_eq!(counter_expected, tree.next_table_id());
    }

    Ok(())
}
