use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, KvSeparationOptions, MemoryVersionSetProvider,
    SeqNo,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_guard_into_inner_if() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("earth#name", "earth", 0);
    tree.insert("earth#color", "BLUE", 0);

    assert_eq!(2, tree.iter(SeqNo::MAX, None).count());

    assert_eq!(
        1,
        tree.iter(SeqNo::MAX, None)
            .filter_map(|guard| {
                guard
                    .into_inner_if(|key| key.ends_with(b"#name"))
                    .unwrap()
                    .1
            })
            .count(),
    );

    Ok(())
}

#[test]
fn tree_guard_into_inner_if_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    tree.insert("earth#name", "earth", 0);
    tree.insert("earth#color", "BLUE", 0);

    assert_eq!(2, tree.iter(SeqNo::MAX, None).count());

    assert_eq!(
        1,
        tree.iter(SeqNo::MAX, None)
            .filter_map(|guard| {
                guard
                    .into_inner_if(|key| key.ends_with(b"#name"))
                    .unwrap()
                    .1
            })
            .count(),
    );

    Ok(())
}
