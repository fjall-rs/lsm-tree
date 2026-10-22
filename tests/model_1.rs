// Found by model testing

use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, Result, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_model_1() -> Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let value = b"hellohello";

    tree.insert(b"a", value, seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.insert(b"b", value, seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.remove(b"b", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let snapshot_seqno = seqno.get();

    tree.insert(b"c", value, seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Leveled::default()),
        seqno.next(),
        0,
    )?;

    {
        log::info!(r#"Getting "b""#);
        assert!(!tree.contains_key(b"b", snapshot_seqno)?);
    }

    Ok(())
}
