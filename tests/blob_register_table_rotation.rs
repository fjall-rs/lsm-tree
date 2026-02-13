use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, KvSeparationOptions,
    SequenceNumberCounter,
};
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

    let tree = lsm_tree::Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(0)
            .age_cutoff(1.0)
            .staleness_threshold(0.0),
    ))
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);

    tree.flush_active_memtable(0)?;
    tree.major_compact(1, 0)?;

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

    let tree = lsm_tree::Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(0)
            .age_cutoff(1.0)
            .staleness_threshold(0.0),
    ))
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0); // f is not overwritten

    tree.flush_active_memtable(0)?;
    tree.major_compact(1, 0)?;

    tree.insert("a", "a", 1);
    tree.insert("b", "b", 1);
    tree.insert("c", "c", 1);
    tree.insert("d", "d", 1);
    tree.insert("e", "e", 1);

    tree.flush_active_memtable(0)?;
    tree.major_compact(1, 10)?;

    assert_eq!(6, tree.table_count());
    assert_eq!(2, tree.blob_file_count());

    tree.major_compact(1, 11)?;

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
