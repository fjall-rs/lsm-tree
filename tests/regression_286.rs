use lsm_tree::{AbstractTree, MemoryVersionSetProvider};
use std::sync::Arc;

#[test_log::test]
#[ignore = "restore clear"]
fn regression_286() -> lsm_tree::Result<()> {
    let folder = lsm_tree::get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
            .open()?;

        tree.insert("foo", b"1", 0);
        tree.clear()?;
    }

    {
        let _tree = lsm_tree::Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
            .open()?;
    }

    Ok(())
}
