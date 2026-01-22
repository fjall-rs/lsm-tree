use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn blob_tree_guarded_size() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()?;

    tree.insert("a".as_bytes(), "abc", 0);
    tree.insert("b".as_bytes(), "a".repeat(10_000), 0);

    assert_eq!(
        10_003u32,
        tree.iter(SeqNo::MAX, None).flat_map(Guard::size).sum(),
    );

    Ok(())
}
