use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};

#[test]
fn ingestion_dirty_snapshot() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.insert("a", "b", seqno.next());

    let snapshot_seqno = 1;
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    let mut ingest = tree.ingestion()?;
    ingest.write("b", "b")?;
    ingest.finish()?;

    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}
