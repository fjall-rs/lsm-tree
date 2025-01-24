use lsm_tree::{AbstractTree, Config};
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open()?;

    tree.bulk_ingest((0..ITEM_COUNT as u64).map(|x| {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        (k.into(), v.into())
    }))?;

    assert_eq!(tree.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );

    Ok(())
}
