use lsm_tree::Config;
use test_log::test;

#[test]
fn recover_from_different_folder() -> lsm_tree::Result<()> {
    if std::path::Path::new(".test").try_exists()? {
        std::fs::remove_dir_all(".test")?;
    }

    let folder = ".test/asd";

    {
        let tree = Config::new(folder).open()?;

        tree.insert("abc", "def", 0);
        tree.insert("wqewe", "def", 0);
        tree.insert("ewewq", "def", 0);
        tree.insert("asddas", "def", 0);
        tree.insert("ycxycx", "def", 0);
        tree.insert("asdsda", "def", 0);
        tree.insert("wewqe", "def", 0);

        tree.flush_active_memtable()?;
    }

    {
        let _tree = Config::new(folder).open()?;
    }

    let absolute_folder = std::path::Path::new(folder).canonicalize()?;

    std::fs::create_dir_all(".test/def")?;
    std::env::set_current_dir(".test/def")?;

    {
        let tree = Config::new(&absolute_folder).open()?;

        tree.insert("abc", "def", 0);
        tree.insert("wqewe", "def", 0);
        tree.insert("ewewq", "def", 0);
        tree.insert("asddas", "def", 0);
        tree.insert("ycxycx", "def", 0);
        tree.insert("asdsda", "def", 0);
        tree.insert("wewqe", "def", 0);

        tree.flush_active_memtable()?;
    }

    for _ in 0..10 {
        let _tree = Config::new(&absolute_folder).open()?;
    }

    /* let temp_folder = "/devssd/code/rust/fjall/def";

    std::fs::create_dir_all(temp_folder)?;

    for _ in 0..2 {
        std::env::set_current_dir(&temp_folder)?;

        let _keyspace = Config::new("/devssd/code/rust/fjall/asd")
            .max_write_buffer_size(1)
            .open()?;
        std::thread::sleep(Duration::from_secs(1));
    } */

    Ok(())
}
