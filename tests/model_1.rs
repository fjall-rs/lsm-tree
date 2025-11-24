// Found by model testing

use lsm_tree::{get_tmp_folder, AbstractTree, Result, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn model_1() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    let value = b"hellohello";

    tree.insert(b"a", value, 0);
    tree.flush_active_memtable(0)?;

    tree.insert(b"b", value, 1);
    tree.flush_active_memtable(0)?;

    tree.remove(b"b", 2);
    tree.flush_active_memtable(0)?;

    tree.insert(b"c", value, 3);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;

    {
        log::info!(r#"Getting "b""#);
        let seqno = 5;
        assert!(!tree.contains_key(b"b", seqno)?);
    }

    Ok(())
}
