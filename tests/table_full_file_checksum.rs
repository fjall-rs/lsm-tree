use lsm_tree::{AbstractTree, Config};
use test_log::test;
use xxhash_rust::xxh3::xxh3_128;

#[test]
fn table_full_file_checksum() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    {
        let tree = Config::new(&folder).open()?;

        for key in ('a'..='z').map(|c| c.to_string()) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;

        let version = tree.current_version();
        let table = version.iter_segments().next().unwrap();

        let expected_checksum = *table.checksum();
        let real_checksum = xxh3_128(&std::fs::read(&*table.path)?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    {
        let tree = Config::new(&folder).open()?;

        let version = tree.current_version();
        let table = version.iter_segments().next().unwrap();

        let expected_checksum = *table.checksum();
        let real_checksum = xxh3_128(&std::fs::read(&*table.path)?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    Ok(())
}
