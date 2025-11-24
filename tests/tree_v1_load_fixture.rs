use lsm_tree::{Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_load_v1() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree";

    let result = Config::new(&folder, SequenceNumberCounter::default()).open();

    matches!(result, Err(lsm_tree::Error::InvalidVersion(1)));

    Ok(())
}

#[test]
fn tree_load_v1_corrupt() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree_corrupt";

    let result = Config::new(&folder, SequenceNumberCounter::default()).open();

    matches!(result, Err(lsm_tree::Error::InvalidVersion(1)));

    Ok(())
}
