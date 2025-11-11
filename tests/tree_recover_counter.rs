use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_recover_table_counter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let counter_expected = {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;

        assert_eq!(0, tree.next_table_id());

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(2, tree.next_table_id());

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.next_table_id()
    };

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
        assert_eq!(counter_expected, tree.next_table_id());
    }

    Ok(())
}
