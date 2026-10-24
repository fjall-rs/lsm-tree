use fs_extra::dir::CopyOptions;
use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn tree_reload_pwd() -> lsm_tree::Result<()> {
    let folder_old = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    {
        let tree = Config::new(&folder_old, version_set_provider.clone()).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(ITEM_COUNT, tree.len(SeqNo::MAX, None)?);
    }

    let folder_new = tempfile::tempdir()?;
    let folder_new_subfolder = folder_new.path().join("deep");
    std::fs::create_dir_all(&folder_new_subfolder)?;

    fs_extra::dir::move_dir(
        folder_old,
        &folder_new_subfolder,
        &CopyOptions::default().content_only(true).overwrite(true),
    )
    .expect("should move");

    {
        let tree = Config::new(&folder_new_subfolder, version_set_provider).open()?;
        assert_eq!(ITEM_COUNT, tree.len(SeqNo::MAX, None)?);
    }

    Ok(())
}
