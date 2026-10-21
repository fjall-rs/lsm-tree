use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_recover_large_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        tree.insert("a", "a".repeat(100_000), 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;
    }

    for _ in 0..3 {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        assert_eq!(
            &*tree.get("a", SeqNo::MAX)?.expect("should exist"),
            "a".repeat(100_000).as_bytes()
        );
    }

    Ok(())
}
