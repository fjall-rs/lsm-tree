use lsm_tree::{AbstractTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use std::path::Path;
use test_log::test;

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

// v4 supports v3 blob files, so we need to check
#[test]
fn tree_blob_load_v3() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = Path::new("test_fixture/v3_blob_tree");

    // Copy the fixture data, so we can safely mutate it
    copy_dir_all(folder, &dir)?;

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    assert_eq!(
        Some(b"abc".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;

    tree.insert("a", "def", 1);
    tree.flush_active_memtable(SeqNo::MAX)?;

    assert_eq!(
        Some(b"def".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;

    assert_eq!(
        Some(b"def".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    Ok(())
}
