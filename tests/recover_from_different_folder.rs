use lsm_tree::{AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn recover_from_different_folder() -> lsm_tree::Result<()> {
    if std::path::Path::new(".test").try_exists()? {
        std::fs::remove_dir_all(".test")?;
    }

    let folder = ".test/asd";
    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    {
        let tree = Config::new(folder, version_set_provider.clone()).open()?;

        for key in [
            "abc", "wqewe", "ewewq", "asddas", "ycxycx", "asdsda", "wewqe",
        ] {
            tree.insert(key, "def", seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    }

    {
        let _tree = Config::new(folder, version_set_provider.clone()).open()?;
    }

    let absolute_folder = std::path::Path::new(folder).canonicalize()?;

    std::fs::create_dir_all(".test/def")?;
    std::env::set_current_dir(".test/def")?;

    {
        let tree = Config::new(&absolute_folder, version_set_provider.clone()).open()?;

        for key in [
            "abc", "wqewe", "ewewq", "asddas", "ycxycx", "asdsda", "wewqe",
        ] {
            tree.insert(key, "def", seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    }

    for _ in 0..10 {
        let _tree = Config::new(&absolute_folder, version_set_provider.clone()).open()?;
    }

    Ok(())
}
