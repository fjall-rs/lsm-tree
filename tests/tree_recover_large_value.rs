use lsm_tree::{AbstractTree, Config, SeqNo};
use test_log::test;

#[test]
fn tree_recover_large_value() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;
        tree.insert("a", "a".repeat(100_000), 0);
        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(&folder).open()?;
        assert_eq!(
            &*tree.get("a", SeqNo::MAX)?.expect("should exist"),
            "a".repeat(100_000).as_bytes()
        );
    }

    Ok(())
}
