use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
#[ignore]
fn tree_bulk_ingest() -> lsm_tree::Result<()> {
    todo!()

    // let folder = tempfile::tempdir()?;

    // let tree = Config::new(folder).open()?;

    // let seqno = SequenceNumberCounter::default();
    // let visible_seqno = SequenceNumberCounter::default();

    // tree.ingest(
    //     (0..ITEM_COUNT as u64).map(|x| {
    //         let k = x.to_be_bytes();
    //         let v = nanoid::nanoid!();
    //         (k.into(), v.into())
    //     }),
    //     &seqno,
    //     &visible_seqno,
    // )?;

    // assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    // assert_eq!(
    //     tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
    //     ITEM_COUNT,
    // );
    // assert_eq!(
    //     tree.iter(SeqNo::MAX, None)
    //         .rev()
    //         .flat_map(|x| x.key())
    //         .count(),
    //     ITEM_COUNT,
    // );

    // Ok(())
}

#[test]
#[ignore]
fn tree_copy() -> lsm_tree::Result<()> {
    todo!()

    //     let folder = tempfile::tempdir()?;
    //     let src = Config::new(folder).open()?;

    //     let seqno = SequenceNumberCounter::default();
    //     let visible_seqno = SequenceNumberCounter::default();

    //     src.ingest(
    //         (0..ITEM_COUNT as u64).map(|x| {
    //             let k = x.to_be_bytes();
    //             let v = nanoid::nanoid!();
    //             (k.into(), v.into())
    //         }),
    //         &seqno,
    //         &visible_seqno,
    //     )?;

    //     assert_eq!(src.len(SeqNo::MAX, None)?, ITEM_COUNT);
    //     assert_eq!(
    //         src.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
    //         ITEM_COUNT,
    //     );
    //     assert_eq!(
    //         src.iter(SeqNo::MAX, None)
    //             .rev()
    //             .flat_map(|x| x.key())
    //             .count(),
    //         ITEM_COUNT,
    //     );
    //     assert!(src.lock_active_memtable().is_empty());

    //     let folder = tempfile::tempdir()?;
    //     let dest = Config::new(folder).open()?;

    //     dest.ingest(
    //         src.iter(SeqNo::MAX, None)
    //             .map(|x| x.into_inner())
    //             .map(|x| x.unwrap()),
    //         &seqno,
    //         &visible_seqno,
    //     )?;

    //     assert_eq!(dest.len(SeqNo::MAX, None)?, ITEM_COUNT);
    //     assert_eq!(
    //         dest.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
    //         ITEM_COUNT,
    //     );
    //     assert_eq!(
    //         dest.iter(SeqNo::MAX, None)
    //             .rev()
    //             .flat_map(|x| x.key())
    //             .count(),
    //         ITEM_COUNT,
    //     );
    //     assert!(dest.lock_active_memtable().is_empty());

    //     Ok(())
    // }

    // #[test]
    // #[ignore]
    // fn blob_tree_bulk_ingest() -> lsm_tree::Result<()> {
    //     let folder = tempfile::tempdir()?;

    //     let tree = Config::new(folder)
    //         .blob_file_separation_threshold(1)
    //         .open_as_blob_tree()?;

    //     let seqno = SequenceNumberCounter::default();
    //     let visible_seqno = SequenceNumberCounter::default();

    //     tree.ingest(
    //         (0..ITEM_COUNT as u64).map(|x| {
    //             let k = x.to_be_bytes();
    //             let v = nanoid::nanoid!();
    //             (k.into(), v.into())
    //         }),
    //         &seqno,
    //         &visible_seqno,
    //     )?;

    //     assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    //     assert_eq!(
    //         tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
    //         ITEM_COUNT,
    //     );
    //     assert_eq!(
    //         tree.iter(SeqNo::MAX, None)
    //             .rev()
    //             .flat_map(|x| x.key())
    //             .count(),
    //         ITEM_COUNT,
    //     );
    //     assert_eq!(1, tree.blob_file_count());

    //     Ok(())
}

#[test]
#[ignore]
fn blob_tree_copy() -> lsm_tree::Result<()> {
    todo!()

    // let folder = tempfile::tempdir()?;
    // let src = Config::new(folder)
    //     .blob_file_separation_threshold(1)
    //     .open_as_blob_tree()?;

    // let seqno = SequenceNumberCounter::default();
    // let visible_seqno = SequenceNumberCounter::default();

    // src.ingest(
    //     (0..ITEM_COUNT as u64).map(|x| {
    //         let k = x.to_be_bytes();
    //         let v = nanoid::nanoid!();
    //         (k.into(), v.into())
    //     }),
    //     &seqno,
    //     &visible_seqno,
    // )?;

    // assert_eq!(src.len(SeqNo::MAX, None)?, ITEM_COUNT);
    // assert_eq!(
    //     src.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
    //     ITEM_COUNT,
    // );
    // assert_eq!(
    //     src.iter(SeqNo::MAX, None)
    //         .rev()
    //         .flat_map(|x| x.key())
    //         .count(),
    //     ITEM_COUNT,
    // );
    // assert!(src.lock_active_memtable().is_empty());
    // assert_eq!(1, src.blob_file_count());

    // let folder = tempfile::tempdir()?;
    // let dest = Config::new(folder)
    //     .blob_file_separation_threshold(1)
    //     .open_as_blob_tree()?;

    // dest.ingest(
    //     src.iter(SeqNo::MAX, None)
    //         .map(|x| x.into_inner())
    //         .map(|x| x.unwrap()),
    //     &seqno,
    //     &visible_seqno,
    // )?;

    // assert_eq!(dest.len(SeqNo::MAX, None)?, ITEM_COUNT);
    // assert_eq!(
    //     dest.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
    //     ITEM_COUNT,
    // );
    // assert_eq!(
    //     dest.iter(SeqNo::MAX, None)
    //         .rev()
    //         .flat_map(|x| x.key())
    //         .count(),
    //     ITEM_COUNT,
    // );
    // assert!(dest.lock_active_memtable().is_empty());
    // assert_eq!(1, dest.blob_file_count());

    // Ok(())
}
