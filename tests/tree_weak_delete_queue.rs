use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn tree_weak_delete_queue() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    assert_eq!(b"a", &*tree.first_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("a", 1);
    assert_eq!(b"b", &*tree.first_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("b", 1);
    assert_eq!(b"c", &*tree.first_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("c", 1);
    assert_eq!(b"d", &*tree.first_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("d", 1);
    assert_eq!(b"e", &*tree.first_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("e", 1);
    assert!(tree.is_empty(None, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_queue_reverse() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    assert_eq!(b"e", &*tree.last_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("e", 1);
    assert_eq!(b"d", &*tree.last_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("d", 1);
    assert_eq!(b"c", &*tree.last_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("c", 1);
    assert_eq!(b"b", &*tree.last_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("b", 1);
    assert_eq!(b"a", &*tree.last_key_value(None, None).unwrap().unwrap().0);

    tree.remove_weak("a", 1);
    assert!(tree.is_empty(None, None)?);

    Ok(())
}
