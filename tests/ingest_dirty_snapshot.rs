use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;

#[test]
fn ingestion_dirty_snapshot() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.insert("a", "b", seqno.next());

    let snapshot_seqno = 1;
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    let mut ingest = tree.ingestion()?;
    ingest.write("b", "b")?;
    let (ingestion_result, ingestion_seqno) = ingest
        .finish(
            || version_set_provider.flush_active_memtable(&tree, seqno.next(), 0),
            || seqno.next(),
        )?
        .expect("ingestion should contain writes");
    version_set_provider.register_flush(ingestion_result, ingestion_seqno);

    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}
