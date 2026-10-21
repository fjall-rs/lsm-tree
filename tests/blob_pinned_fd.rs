use lsm_tree::{
    get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_with_pinned_fd() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .use_descriptor_table(None)
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        let batch_seqno = seqno.next();
        tree.insert("big", &big_value, batch_seqno);
        tree.insert("smol", "small value", batch_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
    }

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .use_descriptor_table(None)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
    }

    Ok(())
}
