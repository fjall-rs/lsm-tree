// Found by model testing

use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, KvSeparationOptions, Result,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn model_4() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::<lsm_tree::fs::StdFileSystem>::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
    .data_block_size_policy(BlockSizePolicy::all(100))
    .open()?;

    let compaction =
        Arc::new(lsm_tree::compaction::Leveled::default().with_table_target_size(1_000));

    let value = b"hellohello";

    tree.insert([0], value, 132);
    tree.insert([0], value, 133);
    tree.insert([1], value, 134);
    tree.insert([2], value, 135);
    tree.insert([3], value, 136);
    tree.insert([4], value, 137);
    tree.insert([5], value, 138);
    tree.insert([6], value, 139);
    tree.insert([7], value, 140);
    tree.insert([0, 0, 0, 0, 0, 0, 9, 217], value, 141);
    tree.insert([0, 0, 0, 0, 0, 0, 2, 77], value, 142);
    tree.insert([0, 0, 0, 0, 0, 0, 33, 92], value, 143);
    tree.insert([0, 0, 0, 0, 0, 0, 38, 41], value, 144);
    tree.insert([0, 0, 0, 0, 0, 0, 22, 143], value, 145);
    tree.insert([0, 0, 0, 0, 0, 0, 22, 161], value, 146);
    tree.insert([0, 0, 0, 0, 0, 0, 9, 143], value, 148);
    tree.insert([0, 0, 0, 0, 0, 0, 25, 222], value, 149);
    tree.insert([0, 0, 0, 0, 0, 0, 11, 144], value, 150);
    tree.insert([0, 0, 0, 0, 0, 0, 8, 208], value, 151);
    tree.insert([0, 0, 0, 0, 0, 0, 31, 195], value, 152);
    tree.insert([0, 0, 0, 0, 0, 0, 27, 47], value, 153);
    tree.insert([0, 0, 0, 0, 0, 0, 31, 104], value, 154);
    tree.insert([0, 0, 0, 0, 0, 0, 14, 219], value, 155);
    tree.insert([0, 0, 0, 0, 0, 0, 17, 125], value, 156);
    tree.insert([0, 0, 0, 0, 0, 0, 15, 52], value, 157);
    tree.insert([0, 0, 0, 0, 0, 0, 20, 230], value, 158);
    tree.insert([0, 0, 0, 0, 0, 0, 16, 88], value, 159);
    tree.insert([0, 0, 0, 0, 0, 0, 9, 26], value, 160);
    tree.insert([0, 0, 0, 0, 0, 0, 20, 21], value, 161);
    tree.insert([0, 0, 0, 0, 0, 0, 27, 86], value, 162);
    tree.insert([0, 0, 0, 0, 0, 0, 4, 112], value, 163);
    tree.insert([0, 0, 0, 0, 0, 0, 12, 60], value, 164);
    tree.insert([0, 0, 0, 0, 0, 0, 8, 186], value, 165);
    tree.insert([0, 0, 0, 0, 0, 0, 34, 18], value, 166);
    tree.insert([0, 0, 0, 0, 0, 0, 15, 156], value, 167);
    tree.insert([0, 0, 0, 0, 0, 0, 5, 91], value, 168);
    tree.insert([0, 0, 0, 0, 0, 0, 36, 0], value, 169);
    tree.insert([0, 0, 0, 0, 0, 0, 38, 249], value, 170);
    tree.insert([0, 0, 0, 0, 0, 0, 23, 42], value, 171);
    tree.insert([0, 0, 0, 0, 0, 0, 23, 14], value, 172);
    tree.insert([0, 0, 0, 0, 0, 0, 32, 119], value, 173);
    tree.insert([0, 0, 0, 0, 0, 0, 31, 9], value, 174);
    tree.insert([0, 0, 0, 0, 0, 0, 4, 170], value, 175);
    tree.insert([0, 0, 0, 0, 0, 0, 18, 119], value, 176);
    tree.insert([0, 0, 0, 0, 0, 0, 4, 178], value, 177);
    tree.insert([0, 0, 0, 0, 0, 0, 4, 36], value, 178);
    tree.insert([0, 0, 0, 0, 0, 0, 36, 53], value, 179);
    tree.insert([0, 0, 0, 0, 0, 0, 35, 157], value, 181);
    tree.insert([0, 0, 0, 0, 0, 0, 22, 24], value, 182);
    tree.insert([0, 0, 0, 0, 0, 0, 33, 247], value, 183);
    tree.insert([0, 0, 0, 0, 0, 0, 26, 236], value, 185);
    tree.flush_active_memtable(86)?;

    tree.insert([0], value, 186);
    tree.flush_active_memtable(87)?;

    tree.insert([0, 0, 0, 0, 0, 0, 7, 49], value, 187);
    tree.flush_active_memtable(88)?;

    tree.insert([0, 0, 0, 0, 0, 0, 18, 134], value, 188);
    tree.flush_active_memtable(89)?;
    tree.compact(compaction.clone(), 89)?;

    tree.insert([0], value, 189);
    tree.flush_active_memtable(90)?;

    tree.insert([0], value, 190);
    tree.flush_active_memtable(91)?;

    tree.insert([0], value, 191);
    tree.flush_active_memtable(92)?;

    tree.insert([0], value, 192);
    tree.flush_active_memtable(93)?;
    tree.compact(compaction.clone(), 93)?;

    tree.drop_range::<&[u8], _>(..)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
