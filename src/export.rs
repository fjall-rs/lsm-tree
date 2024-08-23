// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{AbstractTree, Checksum, KvPair};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    fs::{create_dir_all, File},
    io::{BufWriter, Read, Seek, Write},
    path::Path,
};

const EXPECTED_TRAILER: &[u8; 8] = b"LSMTEXP1";

#[allow(clippy::module_name_repetitions)]
pub fn export_tree<P: AsRef<Path>>(
    path: P,
    items: impl Iterator<Item = crate::Result<KvPair>>,
) -> crate::Result<()> {
    let path = path.as_ref();

    // NOTE: Nothing we can do
    #[allow(clippy::expect_used)]
    let folder = path
        .parent()
        .expect("export path should have parent folder");

    create_dir_all(folder)?;

    let file = File::create(path)?;
    let mut file = BufWriter::new(file);

    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut item_count = 0;

    for kv in items {
        let (k, v) = kv?;

        // NOTE: We know keys are limited to 16-bit length
        #[allow(clippy::cast_possible_truncation)]
        file.write_u16::<BigEndian>(k.len() as u16)?;
        file.write_all(&k)?;

        // NOTE: We know values are limited to 32-bit length
        #[allow(clippy::cast_possible_truncation)]
        file.write_u32::<BigEndian>(v.len() as u32)?;
        file.write_all(&v)?;

        hasher.update(&k);
        hasher.update(&v);
        item_count += 1;
    }

    file.write_u64::<BigEndian>(item_count)?;
    file.write_u64::<BigEndian>(hasher.digest())?;
    file.write_all(EXPECTED_TRAILER)?;

    file.flush()?;
    file.get_mut().sync_all()?;

    Ok(())
}

const TRAILER_SIZE: usize =
    EXPECTED_TRAILER.len() + std::mem::size_of::<u64>() + std::mem::size_of::<u64>();

pub fn import_tree<P: AsRef<Path>>(path: P, tree: &impl AbstractTree) -> crate::Result<()> {
    let mut file = File::open(path)?;

    assert!(
        file.metadata()?.len() >= TRAILER_SIZE as u64,
        "import file is too short"
    );

    // NOTE: Trailer size is trivially small
    #[allow(clippy::cast_possible_wrap)]
    file.seek(std::io::SeekFrom::End(-(TRAILER_SIZE as i64)))?;

    let item_count = file.read_u64::<BigEndian>()?;
    let expected_checksum = file.read_u64::<BigEndian>()?;

    let mut trailer = [0; EXPECTED_TRAILER.len()];
    file.read_exact(&mut trailer)?;

    if &trailer != EXPECTED_TRAILER {
        return Err(crate::Error::Deserialize(
            crate::DeserializeError::InvalidTrailer,
        ));
    }

    file.seek(std::io::SeekFrom::Start(0))?;

    let mut hasher = xxhash_rust::xxh3::Xxh3::new();

    for _ in 0..item_count {
        let klen = file.read_u16::<BigEndian>()?;
        let mut k = vec![0; klen as usize];
        file.read_exact(&mut k)?;

        let vlen = file.read_u32::<BigEndian>()?;
        let mut v = vec![0; vlen as usize];
        file.read_exact(&mut v)?;

        hasher.update(&k);
        hasher.update(&v);

        tree.insert(k, v, 0);
    }

    let checksum = hasher.digest();

    if checksum != expected_checksum {
        return Err(crate::Error::InvalidChecksum((
            Checksum::from_raw(checksum),
            Checksum::from_raw(expected_checksum),
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::AbstractTree;
    use std::{fs::File, io::Write};
    use test_log::test;

    #[test]
    fn import_v1_fixture() -> crate::Result<()> {
        let folder = "test_fixture/v1_export";

        let dir = tempfile::tempdir()?;
        let tree = crate::Config::new(dir.path()).open()?;
        tree.import(folder)?;

        assert_eq!(4, tree.len()?);

        Ok(())
    }

    #[test]
    fn import_v1_fixture_blob() -> crate::Result<()> {
        let folder = "test_fixture/v1_export";

        let dir = tempfile::tempdir()?;
        let tree = crate::Config::new(dir.path()).open_as_blob_tree()?;
        tree.import(folder)?;

        assert_eq!(4, tree.len()?);

        Ok(())
    }

    #[test]
    fn import_v1_fixture_corrupt() -> crate::Result<()> {
        let folder = "test_fixture/v1_export_corrupt";

        let dir = tempfile::tempdir()?;
        let tree = crate::Config::new(dir.path()).open()?;

        assert!(matches!(
            tree.import(folder),
            Err(crate::Error::InvalidChecksum(_))
        ));

        Ok(())
    }

    #[test]
    fn export_v1_fixture() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let tree = crate::Config::new(dir.path()).open()?;

        tree.insert("a", "Oh, don't see you now", 0);
        tree.insert("b", "Wait, don't just give out", 0);
        tree.insert("c", "Move from your old house", 0);
        tree.insert("d", "This city can be so loud", 0);

        let export_path = dir.path().join("export");
        tree.export(&export_path)?;

        assert_eq!(
            std::fs::read("test_fixture/v1_export")?,
            std::fs::read(export_path)?
        );

        Ok(())
    }

    #[test]
    fn export_v1_fixture_blob() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let tree = crate::Config::new(dir.path()).open_as_blob_tree()?;

        tree.insert("a", "Oh, don't see you now", 0);
        tree.insert("b", "Wait, don't just give out", 0);
        tree.insert("c", "Move from your old house", 0);
        tree.insert("d", "This city can be so loud", 0);

        let export_path = dir.path().join("export");
        tree.export(&export_path)?;

        assert_eq!(
            std::fs::read("test_fixture/v1_export")?,
            std::fs::read(export_path)?
        );

        Ok(())
    }

    #[test]
    fn export_import() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path();

        let tree0_path = path.join("a");
        let tree0 = crate::Config::new(tree0_path).open()?;

        tree0.insert("a", "a", 0);
        tree0.insert("b", "b", 0);
        tree0.insert("c", "c", 0);
        tree0.insert("d", "d", 0);
        tree0.insert("e", "e", 0);
        tree0.export(path.join("a.kv"))?;

        let tree1_path = path.join("b");
        let tree1 = crate::Config::new(tree1_path).open()?;

        assert_eq!(0, tree1.len()?);
        tree1.import(path.join("a.kv"))?;
        assert_eq!(tree0.len()?, tree1.len()?);

        Ok(())
    }

    #[test]
    fn export_import_error() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path();
        let kv_file = path.join("fake.kv");

        let mut file = File::create(&kv_file)?;
        file.write_all("hellothisisafakefile".repeat(10).as_bytes())?;
        file.sync_all()?;
        drop(file);

        let tree2_path = path.join("c");
        let tree2 = crate::Config::new(tree2_path).open()?;

        assert!(!matches!(
            tree2.import(kv_file),
            Err(crate::Error::InvalidChecksum(_))
        ));

        Ok(())
    }
}
