use lsm_tree::{AbstractTree, Config, Guard, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(folder, seqno.clone()).open()?;

    let seq = seqno.next();
    let mut ingestion = tree.ingestion()?.with_seqno(seq);
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k.into(), v.into())?;
    }
    ingestion.finish()?;
    visible_seqno.fetch_max(seq + 1);

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        tree.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );

    Ok(())
}

#[test]
fn tree_copy() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let src = Config::new(folder, seqno.clone()).open()?;

    let seq = seqno.next();
    let mut ingestion = src.ingestion()?.with_seqno(seq);
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k.into(), v.into())?;
    }
    ingestion.finish()?;
    visible_seqno.fetch_max(seq + 1);

    assert_eq!(src.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        src.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        src.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );

    let folder = tempfile::tempdir()?;
    let dest = Config::new(folder, seqno.clone()).open()?;

    let seq = seqno.next();
    let mut ingestion = dest.ingestion()?.with_seqno(seq);
    for item in src.iter(SeqNo::MAX, None) {
        let (k, v) = item.into_inner().unwrap();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;
    visible_seqno.fetch_max(seq + 1);

    assert_eq!(dest.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        dest.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        dest.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );

    Ok(())
}

#[test]
fn blob_tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(folder, seqno.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let seq = seqno.next();
    let mut ingestion = tree.ingestion()?.with_seqno(seq);
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k.into(), v.into())?;
    }
    ingestion.finish()?;
    visible_seqno.fetch_max(seq + 1);

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        tree.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );
    assert_eq!(1, tree.blob_file_count());

    Ok(())
}

#[test]
fn blob_tree_copy() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let src = Config::new(folder, seqno.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let seq = seqno.next();
    let mut ingestion = src.ingestion()?.with_seqno(seq);
    for x in 0..ITEM_COUNT as u64 {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        ingestion.write(k.into(), v.into())?;
    }
    ingestion.finish()?;
    visible_seqno.fetch_max(seq + 1);

    assert_eq!(src.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        src.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        src.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );
    assert_eq!(1, src.blob_file_count());

    let folder = tempfile::tempdir()?;
    let dest = Config::new(folder, seqno.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let seq = seqno.next();
    let mut ingestion = dest.ingestion()?.with_seqno(seq);
    for item in src.iter(SeqNo::MAX, None) {
        let (k, v) = item.into_inner().unwrap();
        ingestion.write(k, v)?;
    }
    ingestion.finish()?;
    visible_seqno.fetch_max(seq + 1);

    assert_eq!(dest.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(
        dest.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
        ITEM_COUNT,
    );
    assert_eq!(
        dest.iter(SeqNo::MAX, None)
            .rev()
            .flat_map(|x| x.key())
            .count(),
        ITEM_COUNT,
    );
    assert_eq!(1, dest.blob_file_count());

    Ok(())
}
