use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
#[ignore = "restore Version history maintenance"]
fn tree_recovery_version_free_list() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    {
        let tree = Config::new(path, SequenceNumberCounter::default()).open()?;
        assert!(path.join("v0").try_exists()?);

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.version_free_list_len());
        assert!(path.join("v1").try_exists()?);

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.version_free_list_len());
        assert!(path.join("v2").try_exists()?);
    }

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
        assert_eq!(0, tree.version_free_list_len());
        assert!(!path.join("v0").try_exists()?);
        assert!(!path.join("v1").try_exists()?);
        assert!(path.join("v2").try_exists()?);

        assert!(tree.contains_key("a", 1)?);
        assert!(tree.contains_key("b", 1)?);
    }

    Ok(())
}
