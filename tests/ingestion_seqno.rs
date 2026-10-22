use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;

#[test]
fn ingestion_persisted_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let mut ingest = tree.ingestion()?;
    ingest.write("a", "a")?;
    let (ingestion_result, ingestion_seqno) = ingest
        .finish(
            || version_set_provider.flush_active_memtable(&tree, seqno.next(), 0),
            || seqno.next(),
        )?
        .unwrap();
    version_set_provider.register_flush(ingestion_result, ingestion_seqno);
    assert_eq!(1, tree.table_count());
    assert_eq!(Some(1), tree.get_highest_persisted_seqno());

    let mut ingest = tree.ingestion()?;
    ingest.write("b", "b")?;
    let (ingestion_result, ingestion_seqno) = ingest
        .finish(
            || version_set_provider.flush_active_memtable(&tree, seqno.next(), 0),
            || seqno.next(),
        )?
        .unwrap();
    version_set_provider.register_flush(ingestion_result, ingestion_seqno);
    assert_eq!(2, tree.table_count());
    assert_eq!(Some(3), tree.get_highest_persisted_seqno());

    Ok(())
}
