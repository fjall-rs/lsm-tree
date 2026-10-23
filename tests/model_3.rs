// Found by model testing

use lsm_tree::{
    get_tmp_folder, AbstractTree, AnyTree, KvSeparationOptions, MemoryVersionSetProvider, Result,
    SeqNo, SequenceNumberCounter,
};
use std::{ops::RangeBounds, sync::Arc};
use test_log::test;

fn flush_active_memtable(
    tree: &AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
    gc_watermark: SeqNo,
) -> Result<()> {
    version_set_provider.flush_active_memtable(tree, seqno.next(), gc_watermark)
}

fn compact(
    tree: &AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
    strategy: Arc<dyn lsm_tree::compaction::CompactionStrategy>,
    gc_watermark: SeqNo,
) -> Result<()> {
    version_set_provider.compact(tree, strategy, seqno.next(), gc_watermark)
}

fn drop_range<R: RangeBounds<&'static str>>(
    tree: &AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
    range: R,
) -> Result<()> {
    version_set_provider.drop_range(tree, range, seqno.next(), 0)
}

#[test]
fn model_3() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    let value = b"hellohello";

    tree.insert("a", value, 1);
    tree.insert("i", value, 1);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 0)?;

    tree.insert("a", value, 2);
    tree.insert("f", value, 2);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 0)?;

    tree.insert("a", value, 3);
    tree.insert("h", value, 3);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 0)?;

    tree.insert("a", value, 4);
    tree.insert("b", value, 4);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 0)?;

    tree.insert("c", value, 5);
    tree.insert("g", value, 5);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 0)?;

    tree.insert("b", value, 6);
    tree.insert("c", value, 6);
    tree.insert("d", value, 6);
    tree.insert("d", value, 6);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 15)?;
    compact(&tree, &version_set_provider, &seqno, compaction.clone(), 41)?;

    tree.insert("a", value, 7);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 16)?;

    tree.insert("a", value, 8);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 17)?;

    tree.insert("a", value, 9);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 18)?;

    tree.insert("a", value, 10);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 19)?;
    compact(&tree, &version_set_provider, &seqno, compaction.clone(), 19)?;

    drop_range(&tree, &version_set_provider, &seqno, ..)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
