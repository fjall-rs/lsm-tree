use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_load_v1() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(lsm_tree::Error::InvalidVersion(lsm_tree::FormatVersion::V1))
    );

    Ok(())
}

#[test]
fn tree_load_v1_corrupt() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree_corrupt";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(lsm_tree::Error::InvalidVersion(lsm_tree::FormatVersion::V1))
    );

    Ok(())
}
