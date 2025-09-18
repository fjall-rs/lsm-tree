use lsm_tree::{AbstractTree, Config, Guard, SeqNo};
use test_log::test;

#[test]
#[ignore = "restore"]
fn experimental_blob_tree_guarded_size() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open_as_blob_tree()?;

    tree.insert("a".as_bytes(), "abc", 0);
    tree.insert("b".as_bytes(), "a".repeat(10_000), 0);

    assert_eq!(
        10_003u32,
        tree.iter(SeqNo::MAX, None).flat_map(Guard::size).sum(),
    );

    Ok(())
}
