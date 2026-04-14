use lsm_tree::AbstractTree;

#[test_log::test]
fn regression_286() -> lsm_tree::Result<()> {
    let folder = lsm_tree::get_tmp_folder();
    let seqno = lsm_tree::SequenceNumberCounter::default();

    {
        let tree = lsm_tree::Config::new(
            &folder,
            seqno.clone(),
            lsm_tree::SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
        .open()?;

        tree.insert("foo", b"1", 0);
        tree.clear()?;
    }

    {
        let _tree = lsm_tree::Config::new(
            &folder,
            seqno.clone(),
            lsm_tree::SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
        .open()?;
    }

    Ok(())
}
