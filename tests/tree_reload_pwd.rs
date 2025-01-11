use fs_extra::dir::CopyOptions;
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn tree_reload_pwd() -> lsm_tree::Result<()> {
    let folder_old = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder_old).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        tree.flush_active_memtable(0)?;

        assert_eq!(ITEM_COUNT, tree.len(None, None)?);
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
        let tree = Config::new(&folder_new_subfolder).open()?;
        assert_eq!(ITEM_COUNT, tree.len(None, None)?);
    }

    Ok(())
}
