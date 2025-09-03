use lsm_tree::{AbstractTree, KvPair};
use std::path::Path;
use test_log::test;

fn iterrr(
    path: &Path,
) -> lsm_tree::Result<impl DoubleEndedIterator<Item = lsm_tree::Result<KvPair>>> {
    let tree = lsm_tree::Config::new(path).open()?;

    for x in 0..100u32 {
        let x = x.to_be_bytes();
        tree.insert(x, x, 0);
    }

    Ok(tree.iter(None, None))
}

#[test]
fn tree_iter_lifetime() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir().unwrap();
    assert_eq!(100, iterrr(folder.path())?.count());
    Ok(())
}
