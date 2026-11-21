use lsm_tree::{get_tmp_folder, AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn tree_compaction_cache_eviction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let cache = Arc::new(Cache::with_capacity_bytes(64 * 1_024 * 1_024));

    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .use_cache(cache.clone())
        .open()?;

    for _ in 0..2 {
        for x in 0..ITEM_COUNT as u64 {
            tree.insert(x.to_be_bytes(), x.to_le_bytes(), seqno.next());
        }
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(2, tree.table_count());

    assert_eq!(0, cache.size());

    for x in 0..ITEM_COUNT as u64 {
        assert!(tree.get(x.to_be_bytes(), SeqNo::MAX)?.is_some());
    }
    assert_eq!(ITEM_COUNT, tree.iter(SeqNo::MAX, None).count());

    assert!(cache.size() > 0);

    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    assert_eq!(1, tree.table_count());

    // NOTE: The compacted-away tables are deleted, so their blocks
    // can never be read again and should not occupy the cache
    assert_eq!(0, cache.size());

    for x in 0..ITEM_COUNT as u64 {
        assert!(tree.get(x.to_be_bytes(), SeqNo::MAX)?.is_some());
    }
    assert!(cache.size() > 0);

    Ok(())
}
