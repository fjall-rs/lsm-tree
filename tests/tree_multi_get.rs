use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_multi_get_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 1);
    tree.insert("c", "c", 2);

    // Test getting existing keys
    let keys = vec!["a".as_bytes(), "c".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some("a".as_bytes()));
    assert_eq!(values[1].as_deref(), Some("c".as_bytes()));

    // Test getting a mix of existing and non-existing keys
    let keys = vec!["a".as_bytes(), "d".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some("a".as_bytes()));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref(), Some("b".as_bytes()));

    // Test getting non-existing key
    let keys = vec!["d".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 1);
    assert_eq!(values[0], None);

    // Test with flush
    tree.flush_active_memtable(2)?;

    let keys = vec!["a".as_bytes(), "d".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some("a".as_bytes()));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref(), Some("b".as_bytes()));

    Ok(())
}

#[test]
fn tree_multi_get_overwrite() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a_old", 0);
    tree.insert("b", "b", 1);
    tree.insert("a", "a_new", 2);
    tree.insert("c", "c", 3);

    // Test getting overwriten keys
    let keys = vec!["a".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some("a_new".as_bytes()));
    assert_eq!(values[1].as_deref(), Some("b".as_bytes()));

    // Test with flush
    tree.flush_active_memtable(3)?;

    let keys = vec!["a".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some("a_new".as_bytes()));
    assert_eq!(values[1].as_deref(), Some("b".as_bytes()));

    Ok(())
}

#[test]
fn tree_multi_get_consistency() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 1);

    // Compare with get
    let multi_get_val = tree.multi_get(&["a".as_bytes()], SeqNo::MAX)?;
    let get_val = tree.get("a", SeqNo::MAX)?;

    assert_eq!(multi_get_val.len(), 1);
    assert_eq!(multi_get_val[0], get_val);

    // Compare with get on non-existing key
    let multi_get_val = tree.multi_get(&["c".as_bytes()], SeqNo::MAX)?;
    let get_val = tree.get("c", SeqNo::MAX)?;

    assert_eq!(multi_get_val.len(), 1);
    assert_eq!(multi_get_val[0], get_val);

    Ok(())
}
