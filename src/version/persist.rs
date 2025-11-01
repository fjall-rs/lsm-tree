use crate::{
    file::{fsync_directory, rewrite_atomic},
    version::Version,
};
use std::{io::BufWriter, path::Path};

pub fn persist_version(folder: &Path, version: &Version) -> crate::Result<()> {
    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    let path = folder.join(format!("v{}", version.id()));
    let file = std::fs::File::create_new(path)?;
    let writer = BufWriter::new(file);
    let mut writer = sfa::Writer::from_writer(writer);

    version.encode_into(&mut writer)?;

    writer.finish().map_err(|e| match e {
        sfa::Error::Io(e) => crate::Error::from(e),
        _ => unreachable!(),
    })?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder)?;

    rewrite_atomic(&folder.join("current"), &version.id().to_le_bytes())?;

    Ok(())
}
