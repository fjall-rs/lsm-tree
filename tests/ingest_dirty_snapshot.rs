use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

#[test]
fn ingestion_dirty_snapshot() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.insert("a", "b", seqno.next());

    let snapshot_seqno = 1;
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    let mut ingest = tree.ingestion()?.with_seqno(seqno.next());
    ingest.write("b".into(), "b".into())?;
    ingest.finish()?;

    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}
