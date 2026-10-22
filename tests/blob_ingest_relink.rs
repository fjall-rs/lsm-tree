use lsm_tree::{
    get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_ingest_relink() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        let mut ingestion = tree.ingestion()?;
        ingestion.write("big", &big_value)?;
        ingestion.write("smol", "small value")?;
        let (ingestion_result, ingestion_seqno) = ingestion
            .finish(
                || version_set_provider.flush_active_memtable(&tree, seqno.next(), 0),
                || seqno.next(),
            )?
            .expect("ingestion should contain writes");
        version_set_provider.register_flush(ingestion_result, ingestion_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        assert_eq!(
            Some(vec![lsm_tree::table::writer::LinkedFile {
                blob_file_id: 0,
                bytes: big_value.len() as u64,
                on_disk_bytes: big_value.len() as u64,
                len: 1,
            }]),
            tree.current_version()
                .iter_tables()
                .next()
                .unwrap()
                .list_blob_file_references()?,
        );

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(64_000_000)),
            seqno.next(),
            1_000,
        )?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        assert_eq!(
            Some(vec![lsm_tree::table::writer::LinkedFile {
                blob_file_id: 0,
                bytes: big_value.len() as u64,
                on_disk_bytes: big_value.len() as u64,
                len: 1,
            }]),
            tree.current_version()
                .iter_tables()
                .next()
                .unwrap()
                .list_blob_file_references()?,
        );
    }

    Ok(())
}
