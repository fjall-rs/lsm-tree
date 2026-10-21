use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};

#[test]
fn ingestion_persisted_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut ingest = tree.ingestion()?;
    ingest.write("a", "a")?;
    ingest.finish()?;
    assert_eq!(Some(0), tree.get_highest_persisted_seqno());

    let mut ingest = tree.ingestion()?;
    ingest.write("b", "b")?;
    ingest.finish()?;
    assert_eq!(Some(1), tree.get_highest_persisted_seqno());

    Ok(())
}
