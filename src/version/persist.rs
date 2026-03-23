use crate::{
    checksum::ChecksummedWriter,
    file::{fsync_directory, rewrite_atomic, CURRENT_VERSION_FILE},
    fs::{Fs, FsOpenOptions},
    version::Version,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{io::BufWriter, path::Path};

/// Crate-internal (version module is not exported).
pub fn persist_version(
    folder: &Path,
    version: &Version,
    comparator_name: &str,
    fs: &dyn Fs,
) -> crate::Result<()> {
    if comparator_name.len() > crate::comparator::MAX_COMPARATOR_NAME_BYTES {
        return Err(crate::Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "comparator name is {} bytes (max {})",
                comparator_name.len(),
                crate::comparator::MAX_COMPARATOR_NAME_BYTES,
            ),
        )));
    }

    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    let path = folder.join(format!("v{}", version.id()));
    let file = fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
    let writer = BufWriter::new(file);
    let mut writer = ChecksummedWriter::new(writer);

    {
        let mut writer = sfa::Writer::from_writer(&mut writer);

        version.encode_into(&mut writer, comparator_name)?;

        writer.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => unreachable!(),
        })?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(folder, fs)?;
    }

    let checksum = writer.checksum();

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum.into_u128())?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic(
        &folder.join(CURRENT_VERSION_FILE),
        &current_file_content,
        fs,
    )?;

    Ok(())
}
