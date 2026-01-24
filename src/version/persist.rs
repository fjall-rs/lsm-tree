use crate::{
    checksum::ChecksummedWriter,
    file::{fsync_directory, rewrite_atomic, CURRENT_VERSION_FILE},
    fs::FileSystem,
    version::Version,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{io::BufWriter, path::Path};

pub fn persist_version<F: FileSystem>(folder: &Path, version: &Version<F>) -> crate::Result<()> {
    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    let path = folder.join(format!("v{}", version.id()));
    let file = F::create_new(&path)?;
    let writer = BufWriter::new(file);
    let mut writer = ChecksummedWriter::new(writer);

    {
        let mut writer = sfa::Writer::from_writer(&mut writer);

        version.encode_into(&mut writer)?;

        writer.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => unreachable!(),
        })?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory::<F>(folder)?;
    }

    let checksum = writer.checksum();

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum.into_u128())?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic::<F>(&folder.join(CURRENT_VERSION_FILE), &current_file_content)?;

    Ok(())
}
