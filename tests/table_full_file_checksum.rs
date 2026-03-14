use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;
use xxhash_rust::xxh3::xxh3_128;

#[test]
fn table_full_file_checksum() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for key in ('a'..='z').map(|c| c.to_string()) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;

        let version = tree.current_version();
        let table = version.iter_tables().next().unwrap();

        let expected_checksum = table.checksum().into_u128();
        let real_checksum = xxh3_128(&std::fs::read(&*table.path)?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let version = tree.current_version();
        let table = version.iter_tables().next().unwrap();

        let expected_checksum = table.checksum().into_u128();
        let real_checksum = xxh3_128(&std::fs::read(&*table.path)?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    Ok(())
}

#[test]
fn table_full_file_detect_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for key in ('a'..='z').map(|c| c.to_string()) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;

        let version = tree.current_version();
        let table = version.iter_tables().next().unwrap();

        let expected_checksum = table.checksum().into_u128();
        let real_checksum = xxh3_128(&std::fs::read(&*table.path)?);
        assert_eq!(
            real_checksum, expected_checksum,
            "full file checksum mismatch",
        );
    }

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let version = tree.current_version();
        let table = version.iter_tables().next().unwrap();

        {
            use std::io::{Seek, Write};

            let mut f = std::fs::OpenOptions::new().write(true).open(&*table.path)?;

            f.seek(std::io::SeekFrom::Start(100))?;
            f.write_all(b"!")?;
            f.sync_all()?;
        }

        let expected_checksum = table.checksum().into_u128();
        let real_checksum = xxh3_128(&std::fs::read(&*table.path)?);
        assert_ne!(
            real_checksum, expected_checksum,
            "full file checksum should not match",
        );
    }

    Ok(())
}
