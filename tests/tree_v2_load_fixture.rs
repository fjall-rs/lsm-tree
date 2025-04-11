use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_load_v2() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v2_tree";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(lsm_tree::Error::InvalidVersion(lsm_tree::Version::V2))
    );

    Ok(())
}

#[test]
fn tree_load_v2_corrupt() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v2_tree_corrupt";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(lsm_tree::Error::InvalidVersion(lsm_tree::Version::V2))
    );

    Ok(())
}
