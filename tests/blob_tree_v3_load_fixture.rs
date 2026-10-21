use lsm_tree::{AbstractTree, Config, KvSeparationOptions, MemoryVersionSetProvider, SeqNo};
use std::{path::Path, sync::Arc};
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
#[ignore = "create a new provider that reads v3 manifest etc. (we need to provide loading v3 manifests as lsm_tree::legacy anyway)"]
fn tree_blob_load_v3() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = Path::new("test_fixture/v3_blob_tree");

    // Copy the fixture data, so we can safely mutate it
    copy_dir_all(folder, &dir)?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    assert_eq!(
        Some(b"abc".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_000,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_001,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_002,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_003,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_004,
        SeqNo::MAX,
    )?;

    tree.insert("a", "def", 1_005);

    version_set_provider.flush_active_memtable(&tree, 1_006, SeqNo::MAX)?;

    assert_eq!(
        Some(b"def".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_007,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_008,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_009,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_010,
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        1_011,
        SeqNo::MAX,
    )?;

    assert_eq!(
        Some(b"def".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    Ok(())
}
