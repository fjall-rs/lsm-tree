use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_range_memtable_only() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "", 0);
    tree.insert("b", "", 0);
    tree.insert("c", "", 0);

    let found: Vec<String> = tree
        .range("a".."a", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Vec::<String>::new(), found);

    let found = tree
        .range("a"..="a", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a".."b", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a"..="b", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a", "b"], found);

    let found = tree
        .range("a".."a", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .rev()
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Vec::<String>::new(), found);

    let found = tree
        .range("a"..="a", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .rev()
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a".."b", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .rev()
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["a"], found);

    let found = tree
        .range("a"..="b", SeqNo::MAX, None)
        .flat_map(|x| x.key())
        .rev()
        .map(|k| String::from_utf8(k.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vec!["b", "a"], found);

    Ok(())
}
