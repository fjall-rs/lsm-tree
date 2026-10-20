use crate::{
    checksum::ChecksummedWriter,
    file::{fsync_directory, retry_transient_io, rewrite_atomic, CURRENT_VERSION_FILE},
    version::Version,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    io::{BufWriter, Write},
    path::Path,
};

pub fn persist_version(folder: &Path, version: &Version) -> crate::Result<()> {
    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    let path = folder.join(format!("v{}", version.id()));
    let file = retry_transient_io(|| std::fs::File::create(&path))?;
    let writer = BufWriter::new(&file);
    let mut writer = ChecksummedWriter::new(writer);

    {
        let mut writer = sfa::Writer::from_writer(&mut writer);

        version.encode_into(&mut writer)?;

        writer.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => unreachable!(),
        })?;
    }

    writer.flush()?;

    let checksum = writer.checksum();

    drop(writer);

    file.sync_all()?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder)?;

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum.into_u128())?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic(&folder.join(CURRENT_VERSION_FILE), &current_file_content)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TreeType;
    use test_log::test;

    #[test]
    fn version_persist_replaces_orphaned_file() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let version = Version::new(0, TreeType::Standard);

        // Simulates the leftover of a persist that failed midway
        std::fs::write(dir.path().join("v0"), b"partial")?;

        persist_version(dir.path(), &version)?;

        assert_ne!(
            b"partial".as_slice(),
            &*std::fs::read(dir.path().join("v0"))?
        );
        assert!(dir.path().join(CURRENT_VERSION_FILE).try_exists()?);

        Ok(())
    }
}
