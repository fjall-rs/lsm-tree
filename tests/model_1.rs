// Found by model testing

use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, Result};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_model_1() -> Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let value = b"hellohello";

    tree.insert(b"a", value, 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert(b"b", value, 2);

    version_set_provider.flush_active_memtable(&tree, 3, 0)?;

    tree.remove(b"b", 4);

    version_set_provider.flush_active_memtable(&tree, 5, 0)?;

    tree.insert(b"c", value, 6);

    version_set_provider.flush_active_memtable(&tree, 7, 0)?;

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Leveled::default()),
        8,
        0,
    )?;

    {
        log::info!(r#"Getting "b""#);
        let seqno = 5;
        assert!(!tree.contains_key(b"b", seqno)?);
    }

    Ok(())
}
