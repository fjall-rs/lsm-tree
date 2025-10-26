use lsm_tree::{AbstractTree, Config, KvSeparationOptions};
use test_log::test;
use xxhash_rust::xxh3::xxh3_128;

#[test]
fn blob_file_full_file_checksum() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    {
        let tree = Config::new(&folder)
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        for key in ('a'..='z').map(|c| c.to_string()) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;

        let version = tree.current_version();
        let blob_file = version.blob_files.iter().next().unwrap();

        let expected_checksum = *blob_file.checksum();
        let real_checksum = xxh3_128(&std::fs::read(blob_file.path())?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    {
        let tree = Config::new(&folder).open()?;

        let version = tree.current_version();
        let blob_file = version.blob_files.iter().next().unwrap();

        let expected_checksum = *blob_file.checksum();
        let real_checksum = xxh3_128(&std::fs::read(blob_file.path())?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    Ok(())
}
