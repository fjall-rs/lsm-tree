// Found by model testing

use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, AnyTree, KvSeparationOptions,
    MemoryVersionSetProvider, Result, SeqNo, SequenceNumberCounter,
};
use std::{ops::RangeBounds, sync::Arc};
use test_log::test;

fn flush_active_memtable(
    tree: &AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
    gc_watermark: SeqNo,
) -> Result<()> {
    version_set_provider.flush_active_memtable(tree, seqno.next(), gc_watermark)?;
    Ok(())
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
fn model_5() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
        .data_block_size_policy(BlockSizePolicy::all(100))
        .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Leveled::default().with_table_target_size(150));

    let value = b"hellohello";

    tree.insert([0], value, 753);
    tree.insert([0, 0, 0, 0, 0, 0, 1, 0], value, 754);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 140], value, 756);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 127], value, 762);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 98], value, 763);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 138], value, 764);
    tree.insert([0, 0, 0, 0, 0, 0, 3, 150], value, 765);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 23], value, 766);
    tree.insert([0, 0, 0, 0, 0, 0, 3, 121], value, 767);
    tree.insert([0, 0, 0, 0, 0, 0, 1, 212], value, 842);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 152], value, 843);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 241], value, 844);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 798)?;

    tree.insert([0, 0, 0, 0, 0, 0, 3, 120], value, 898);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 799)?;

    tree.insert([0, 0, 0, 0, 0, 0, 3, 89], value, 899);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 800)?;

    tree.insert([0, 0, 0, 0, 0, 0, 1, 52], value, 901);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 177], value, 902);
    tree.insert([0, 0, 0, 0, 0, 0, 3, 43], value, 903);
    tree.insert([0, 0, 0, 0, 0, 0, 3, 41], value, 904);
    tree.insert([0, 0, 0, 0, 0, 0, 3, 160], value, 905);
    tree.insert([0, 0, 0, 0, 0, 0, 1, 182], value, 906);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 73], value, 907);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 78], value, 912);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 103], value, 913);
    tree.insert([0, 0, 0, 0, 0, 0, 1, 39], value, 914);
    tree.insert([0, 0, 0, 0, 0, 0, 1, 78], value, 927);
    tree.insert([0, 0, 0, 0, 0, 0, 0, 244], value, 928);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 76], value, 929);
    tree.insert([0, 0, 0, 0, 0, 0, 1, 202], value, 934);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 140], value, 936);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 152], value, 937);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 886)?;
    compact(
        &tree,
        &version_set_provider,
        &seqno,
        compaction.clone(),
        886,
    )?;
    assert!(
        tree.level_table_count(6).unwrap() > 0,
        "should have compacted into Lmax",
    );

    tree.insert([0, 0, 0, 0, 0, 0, 3, 145], value, 989);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 890)?;

    tree.insert([0, 0, 0, 0, 0, 0, 3, 99], value, 993);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 894)?;

    tree.insert([0, 0, 0, 0, 0, 0, 1, 106], value, 997);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 898)?;

    tree.insert([0, 0, 0, 0, 0, 0, 2, 99], value, 1001);
    flush_active_memtable(&tree, &version_set_provider, &seqno, 902)?;
    compact(
        &tree,
        &version_set_provider,
        &seqno,
        compaction.clone(),
        902,
    )?;

    drop_range(&tree, &version_set_provider, &seqno, ..)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
