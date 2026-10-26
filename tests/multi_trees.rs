use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_multi_table_ids() -> lsm_tree::Result<()> {
    let folder0 = get_tmp_folder();
    let folder1 = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree0 = Config::new(&folder0, version_set_provider.clone()).open()?;
        assert_eq!(tree0.id(), 0);
        assert_eq!(0, tree0.next_table_id());

        tree0.insert("a", "a", seqno.next());

        version_set_provider.flush_active_memtable(&tree0, seqno.next(), 0)?;

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
                .id,
        );
    }

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree1 = Config::new(&folder1, version_set_provider.clone()).open()?;
        assert_eq!(tree1.id(), 1);
        assert_eq!(0, tree1.next_table_id());

        tree1.insert("a", "a", seqno.next());

        version_set_provider.flush_active_memtable(&tree1, seqno.next(), 0)?;

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
                .id,
        );
    }

    Ok(())
}
