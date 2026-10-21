use lsm_tree::{Config, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_load_v1() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree";

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let result = Config::new(folder, version_set_provider.clone()).open();
    matches!(result, Err(lsm_tree::Error::InvalidVersion(1)));

    Ok(())
}

#[test]
fn tree_load_v1_corrupt() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree_corrupt";

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let result = Config::new(folder, version_set_provider.clone()).open();
    matches!(result, Err(lsm_tree::Error::InvalidVersion(1)));

    Ok(())
}
