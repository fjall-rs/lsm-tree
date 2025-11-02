use byteorder::WriteBytesExt;
use lsm_tree::{file::MANIFEST_FILE, Config, SequenceNumberCounter};
use std::io::Seek;
use test_log::test;

const FUTURE_VERSION: u8 = 255;

#[test]
fn tree_load_future_version() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
    drop(tree);

    {
        let mut manifest = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(folder.path().join(MANIFEST_FILE))?;
        manifest.seek(std::io::SeekFrom::Start(0))?;
        manifest.write_u8(FUTURE_VERSION)?;
        manifest.sync_all()?;
    }

    let result = Config::new(&folder, SequenceNumberCounter::default()).open();
    matches!(result, Err(lsm_tree::Error::InvalidVersion(FUTURE_VERSION)));

    Ok(())
}
