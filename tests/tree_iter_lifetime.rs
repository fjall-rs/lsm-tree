use lsm_tree::{UserKey, UserValue};
use std::path::Path;

fn iterrr(
    path: &Path,
) -> lsm_tree::Result<impl DoubleEndedIterator<Item = lsm_tree::Result<(UserKey, UserValue)>>> {
    let tree = lsm_tree::Config::new(path).open()?;

    for x in 0..100u32 {
        let x = x.to_be_bytes();
        tree.insert(x, x, 0);
    }

    Ok(tree.iter())
}

#[test_log::test]
fn segment_reader_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir().unwrap();
    assert_eq!(100, iterrr(folder.path())?.count());
    Ok(())
}
