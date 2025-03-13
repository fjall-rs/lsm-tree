use lsm_tree::{AbstractTree, Config, Guard};
use test_log::test;

#[test]
fn experimental_blob_tree_guarded_size() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open_as_blob_tree()?;

    tree.insert("a".as_bytes(), "abc", 0);
    tree.insert("b".as_bytes(), "a".repeat(10_000), 0);

    assert_eq!(
        10_003u32,
        tree.guarded_iter(None, None).flat_map(Guard::size).sum()
    );

    Ok(())
}
