use lsm_tree::{
    config::CompressionPolicy, AbstractTree, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use test_log::test;

// NOTE: This was a logic/MVCC error in v2 that could drop
// a blob file while it was maybe accessible by a snapshot read
//
// https://github.com/fjall-rs/lsm-tree/commit/79c6ead4b955051cbb4835913e21d08b8aeafba1
#[test]
fn blob_gc_seqno_watermark() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .staleness_threshold(0.01)
                .age_cutoff(1.0),
        ))
        .open()?;
    let seqno = SequenceNumberCounter::default();

    tree.insert("a", "neptune".repeat(10_000), seqno.next());

    let snapshot_seqno = seqno.get();

    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(10_000),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune".repeat(10_000),
    );

    tree.insert("a", "neptune2".repeat(10_000), seqno.next());
    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(10_000),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune2".repeat(10_000),
    );

    tree.insert("a", "neptune3".repeat(10_000), seqno.next());
    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(10_000),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune3".repeat(10_000),
    );

    tree.flush_active_memtable(0)?;
    assert_eq!(
        &*tree.get("a", snapshot_seqno)?.unwrap(),
        b"neptune".repeat(10_000),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune3".repeat(10_000),
    );

    tree.major_compact(u64::MAX, 0)?;
    tree.major_compact(u64::MAX, 0)?;

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
        b"neptune".repeat(10_000),
    );
    assert_eq!(
        &*tree.get("a", SeqNo::MAX)?.unwrap(),
        b"neptune3".repeat(10_000),
    );

    tree.major_compact(u64::MAX, 1_000)?;

    {
        let gc_stats = tree.current_version().gc_stats().clone();
        assert!(!gc_stats.is_empty());
    }

    tree.major_compact(u64::MAX, 1_000)?;

    {
        let gc_stats = tree.current_version().gc_stats().clone();
        assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
    }

    Ok(())
}
