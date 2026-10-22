use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;

#[test]
fn weak_tombstone_counts_single_pair() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert(b"a", b"old", seqno.next());
    tree.remove_weak(b"a", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.weak_tombstone_count(), 1);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 1);

    Ok(())
}

#[test]
fn weak_tombstone_counts_multiple_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert(b"a", b"old", seqno.next());
    tree.remove_weak(b"a", seqno.next());

    tree.remove_weak(b"b", seqno.next());

    tree.insert(b"c", b"old", seqno.next());
    tree.insert(b"c", b"new", seqno.next());
    tree.remove_weak(b"c", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.weak_tombstone_count(), 3);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 2);

    Ok(())
}

#[test]
fn weak_tombstone_counts_multiple_weak() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert(b"a", b"old", seqno.next());
    tree.remove_weak(b"a", seqno.next());
    tree.remove_weak(b"a", seqno.next());
    tree.remove_weak(b"a", seqno.next());
    tree.remove_weak(b"a", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.weak_tombstone_count(), 4);
    assert_eq!(tree.weak_tombstone_reclaimable_count(), 1); // a:10 is paired with a:11

    Ok(())
}
