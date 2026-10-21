use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, KvSeparationOptions,
    MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

// Force one block per table and one blob per block
//
// Then check if item_count in a table matches the number of referenced blobs (so 1).
//
// See https://github.com/fjall-rs/lsm-tree/commit/0d2d7b2071c65f2538bb01e4512907892991dcbe
#[test]
fn blob_register_table_rotation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(0)
                .age_cutoff(1.0)
                .staleness_threshold(0.0),
        ))
        .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(1)),
        seqno.next(),
        0,
    )?;

    assert_eq!(5, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    for table in tree.current_version().iter_tables() {
        assert_eq!(
            1,
            table
                .list_blob_file_references()?
                .unwrap()
                .iter()
                .map(|x| x.len)
                .sum::<usize>(),
        );
        assert_eq!(
            1,
            table
                .list_blob_file_references()?
                .unwrap()
                .iter()
                .map(|x| x.bytes)
                .sum::<u64>(),
        );
    }

    Ok(())
}

#[test]
fn blob_register_table_rotation_relocation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(0)
                .age_cutoff(1.0)
                .staleness_threshold(0.0),
        ))
        .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);
    tree.insert("f", "f", batch_seqno); // f is not overwritten

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(1)),
        seqno.next(),
        0,
    )?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(1)),
        seqno.next(),
        10,
    )?;

    assert_eq!(6, tree.table_count());
    assert_eq!(2, tree.blob_file_count());

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(1)),
        seqno.next(),
        11,
    )?;

    assert_eq!(6, tree.table_count());
    assert_eq!(2, tree.blob_file_count());

    for table in tree.current_version().iter_tables() {
        assert_eq!(
            1,
            table
                .list_blob_file_references()?
                .unwrap()
                .iter()
                .map(|x| x.len)
                .sum::<usize>(),
        );
        assert_eq!(
            1,
            table
                .list_blob_file_references()?
                .unwrap()
                .iter()
                .map(|x| x.bytes)
                .sum::<u64>(),
        );
    }

    Ok(())
}
