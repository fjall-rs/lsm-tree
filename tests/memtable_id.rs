use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn memtable_id() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);

        assert_eq!(
            0,
            tree.get_version_history_lock()
                .latest_version()
                .active_memtable
                .id()
        );

        tree.flush_active_memtable(0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(
            1,
            tree.get_version_history_lock()
                .latest_version()
                .active_memtable
                .id()
        );
    }

    Ok(())
}
