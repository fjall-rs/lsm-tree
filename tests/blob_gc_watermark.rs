use lsm_tree::{
    config::CompressionPolicy, get_tmp_folder, AbstractTree, Config, KvSeparationOptions,
    MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

// NOTE: This was a logic/MVCC error in v2 that could drop
// a blob file while it was maybe accessible by a snapshot read
//
// https://github.com/fjall-rs/lsm-tree/commit/79c6ead4b955051cbb4835913e21d08b8aeafba1
#[test]
fn blob_gc_seqno_watermark() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .staleness_threshold(0.01)
                .age_cutoff(1.0)
                .separation_threshold(50),
        ))
        .open()?;

    tree.insert("a", "neptune".repeat(50), seqno.next());

    let snapshot_seqno = seqno.get();

    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(50),
    );
    assert_eq!(&*tree.get("a", SeqNo::MAX)?.unwrap(), b"neptune".repeat(50),);

    tree.insert("a", "neptune2".repeat(50), seqno.next());
    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(50),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune2".repeat(50),
    );

    tree.insert("a", "neptune3".repeat(50), seqno.next());
    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(50),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune3".repeat(50),
    );

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(50),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune3".repeat(50),
    );

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
        seqno.next(),
        0,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
        seqno.next(),
        0,
    )?;

    // IMPORTANT: We cannot drop any blobs yet
    // because the watermark is too low
    //
    // This would previously fail

    {
        let gc_stats = tree.current_version().gc_stats().clone();
        assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
    }

    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(50),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune3".repeat(50),
    );

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
        seqno.next(),
        1_000,
    )?;

    {
        let gc_stats = tree.current_version().gc_stats().clone();
        assert!(!gc_stats.is_empty());
    }

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
        seqno.next(),
        1_000,
    )?;

    {
        let gc_stats = tree.current_version().gc_stats().clone();
        assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
    }

    Ok(())
}
