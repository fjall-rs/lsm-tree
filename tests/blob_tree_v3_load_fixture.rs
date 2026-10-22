use lsm_tree::{
    AbstractTree, Cache, Config, DescriptorTable, KvSeparationOptions, MemoryVersionSetProvider,
    SeqNo, SequenceNumberCounter,
};
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
fn tree_blob_load_v3() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = Path::new("test_fixture/v3_blob_tree");

    // Copy the fixture data, so we can safely mutate it
    copy_dir_all(folder, &dir)?;

    let cache = Arc::new(Cache::with_capacity_bytes(1_000));
    let descriptor_table = Arc::new(DescriptorTable::new(10));

    let current_version = lsm_tree::legacy::v3::load_manifest(&dir, &cache, &descriptor_table)?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::with_initial_version(
        current_version,
    ));

    let seqno = SequenceNumberCounter::new(1_000);

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
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;

    tree.insert("a", "def", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), SeqNo::MAX)?;

    assert_eq!(
        Some(b"def".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        SeqNo::MAX,
    )?;

    assert_eq!(
        Some(b"def".as_slice()),
        tree.get(b"a", SeqNo::MAX)?.as_deref(),
    );

    Ok(())
}
