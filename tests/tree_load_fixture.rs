use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_load_v1() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree";

    let tree = Config::new(folder).open()?;
    assert_eq!(8, tree.len()?);

    assert_eq!(0, tree.verify()?);

    Ok(())
}
