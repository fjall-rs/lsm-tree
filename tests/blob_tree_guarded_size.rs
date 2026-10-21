use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_guarded_size() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a".as_bytes(), "abc", batch_seqno);
    tree.insert("b".as_bytes(), "a".repeat(10_000), batch_seqno);

    assert_eq!(
        10_003u32,
        tree.iter(SeqNo::MAX, None).flat_map(Guard::size).sum(),
    );

    Ok(())
}
