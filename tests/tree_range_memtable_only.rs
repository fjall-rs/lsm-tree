use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_range_memtable_only() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open()?;

    tree.insert("a", "", 0);
    tree.insert("b", "", 0);
    tree.insert("c", "", 0);

    let found = tree
        .range("a".."a")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Vec::<String>::new(), found);

    let found = tree
        .range("a"..="a")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a".."b")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a"..="b")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a", "b"], found);

    let found = tree
        .range("a".."a")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Vec::<String>::new(), found);

    let found = tree
        .range("a"..="a")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a".."b")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a"..="b")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["b", "a"], found);

    Ok(())
}

#[test]
fn tree_snapshot_range_memtable_only() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open()?;

    tree.insert("a", "", 5);
    tree.insert("b", "", 5);
    tree.insert("c", "", 5);

    let snapshot = tree.snapshot(100);

    let found = snapshot
        .range("a".."a")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Vec::<String>::new(), found);

    let found = snapshot
        .range("a"..="a")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = snapshot
        .range("a".."b")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = snapshot
        .range("a"..="b")
        .flatten()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a", "b"], found);

    let found = snapshot
        .range("a".."a")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Vec::<String>::new(), found);

    let found = snapshot
        .range("a"..="a")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = snapshot
        .range("a".."b")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = snapshot
        .range("a"..="b")
        .flatten()
        .rev()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["b", "a"], found);

    Ok(())
}
