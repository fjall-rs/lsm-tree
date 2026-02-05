use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn recover_from_different_folder() -> lsm_tree::Result<()> {
    if std::path::Path::new(".test").try_exists()? {
        std::fs::remove_dir_all(".test")?;
    }

    let folder = ".test/asd";

    {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("abc", "def", 0);
        tree.insert("wqewe", "def", 0);
        tree.insert("ewewq", "def", 0);
        tree.insert("asddas", "def", 0);
        tree.insert("ycxycx", "def", 0);
        tree.insert("asdsda", "def", 0);
        tree.insert("wewqe", "def", 0);

        tree.flush_active_memtable(0)?;
    }

    {
        let _tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
    }

    let absolute_folder = std::path::Path::new(folder).canonicalize()?;

    std::fs::create_dir_all(".test/def")?;
    std::env::set_current_dir(".test/def")?;

    {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &absolute_folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("abc", "def", 0);
        tree.insert("wqewe", "def", 0);
        tree.insert("ewewq", "def", 0);
        tree.insert("asddas", "def", 0);
        tree.insert("ycxycx", "def", 0);
        tree.insert("asdsda", "def", 0);
        tree.insert("wewqe", "def", 0);

        tree.flush_active_memtable(0)?;
    }

    for _ in 0..10 {
        let _tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &absolute_folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
    }

    Ok(())
}
