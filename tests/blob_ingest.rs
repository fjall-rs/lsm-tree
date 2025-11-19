use lsm_tree::{
    blob_tree::FragmentationEntry, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use test_log::test;

#[test]
fn blob_ingest_gc_stats() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        let mut ingestion = tree.ingestion()?;
        ingestion.write("big", &big_value)?;
        ingestion.write("smol", "small value")?;
        ingestion.finish()?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let mut ingestion = tree.ingestion()?;
        ingestion.write("big", &new_big_value)?;
        ingestion.finish()?;

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0 is expired
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                map.insert(0, FragmentationEntry::new(1, size, size));
                map
            },
            &*gc_stats,
        );
    }

    Ok(())
}
