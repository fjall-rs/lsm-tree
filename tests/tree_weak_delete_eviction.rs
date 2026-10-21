use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_weak_remove_flush_eviction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for c in ('a'..='z').map(|x| (x as u8).to_be_bytes()) {
        tree.insert(c, c, seqno.next());
    }

    for c in ('a'..='z').map(|x| (x as u8).to_be_bytes()) {
        tree.remove_weak(c, seqno.next());
    }

    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // Set GC watermark high enough such that all KVs are under it
    // so all weak tombstones will be matched with their values and be removed
    let gc_watermark = 10_000;

    version_set_provider.flush_active_memtable(&tree, seqno.next(), gc_watermark)?;

    assert_eq!(0, tree.len(SeqNo::MAX, None)?);
    assert_eq!(0, tree.approximate_len());

    Ok(())
}
