use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_load_v1() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree";

    let tree = Config::new(folder).open()?;

    /*  tree.insert("a", "Only ever feeling fine", 0);
    tree.insert("b", "And I'd prefer us to be close", 0);
    tree.insert("c", "I'd like to look you in the eyes, not fearin'", 0);
    tree.insert("d", "Starin' into the void, waitin' for replies", 0);
    tree.insert("e", "Just waitin' for replies", 0);
    tree.insert("f", "---", 0);
    tree.insert("g", "Did you actually load this database?", 0);
    tree.insert("h", "https://www.youtube.com/watch?v=eYS7xmjR4Fk", 0);

    tree.flush_active_memtable()?; */

    assert_eq!(8, tree.len()?);

    assert_eq!(0, tree.verify()?);

    Ok(())
}

#[test]
fn tree_load_v1_corrupt() -> lsm_tree::Result<()> {
    let folder = "test_fixture/v1_tree_corrupt";

    let tree = Config::new(folder).open()?;
    assert_eq!(8, tree.len()?);

    assert_eq!(1, tree.verify()?);

    Ok(())
}
