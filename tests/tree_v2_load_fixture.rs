use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_load_v2() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v2_tree";

    let tree = Config::new(folder).open()?;
    assert_eq!(5, tree.len()?);

    Ok(())
}

#[test]
fn tree_load_v2_corrupt() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v2_tree_corrupt";

    let result = Config::new(folder).open()?;
    assert_eq!(1, result.verify()?);

    Ok(())
}
