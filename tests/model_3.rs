// Found by model testing

use lsm_tree::{AbstractTree, KvSeparationOptions, Result};
use std::sync::Arc;
use test_log::test;

#[test]
fn model_3() -> Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path)
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    let value = b"hellohello";

    tree.insert("a", value, 1);
    tree.insert("i", value, 1);
    tree.flush_active_memtable(0)?;
    eprintln!("==========");
    eprintln!("{:?}", tree.current_version().gc_stats());

    tree.insert("a", value, 2);
    tree.insert("f", value, 2);
    tree.flush_active_memtable(0)?;
    eprintln!("==========");
    eprintln!("{:?}", tree.current_version().gc_stats());

    tree.insert("a", value, 3);
    tree.insert("h", value, 3);
    tree.flush_active_memtable(0)?;
    eprintln!("==========");
    eprintln!("{:?}", tree.current_version().gc_stats());

    tree.insert("a", value, 4);
    tree.insert("b", value, 4);
    tree.flush_active_memtable(0)?;
    eprintln!("==========");
    eprintln!("{:?}", tree.current_version().gc_stats());

    tree.insert("c", value, 5);
    tree.insert("g", value, 5);
    tree.flush_active_memtable(0)?;
    eprintln!("==========");
    eprintln!("{:?}", tree.current_version().gc_stats());

    tree.insert("b", value, 6);
    tree.insert("c", value, 6);
    tree.insert("d", value, 6);
    tree.insert("e", value, 6);
    tree.flush_active_memtable(15)?;
    tree.compact(compaction.clone(), 41)?;
    eprintln!("==========");
    eprintln!("{:#?}", tree.current_version().gc_stats());

    tree.insert("a", value, 7);
    tree.flush_active_memtable(16)?;
    eprintln!("==========");
    eprintln!("{:#?}", tree.current_version().gc_stats());

    tree.insert("a", value, 8);
    tree.flush_active_memtable(17)?;
    eprintln!("==========");
    eprintln!("{:#?}", tree.current_version().gc_stats());

    tree.insert("a", value, 9);
    tree.flush_active_memtable(18)?;
    eprintln!("==========");
    eprintln!("{:#?}", tree.current_version().gc_stats());

    tree.insert("a", value, 10);
    tree.flush_active_memtable(19)?;
    tree.compact(compaction.clone(), 19)?;
    eprintln!("==========");
    eprintln!("{:#?}", tree.current_version().gc_stats());

    tree.drop_range::<&[u8], _>(..)?;
    eprintln!("==========");
    eprintln!("{:?}", tree.current_version().gc_stats());

    eprintln!(
        "{:?}",
        tree.current_version()
            .value_log
            .values()
            .map(|x| x.id())
            .collect::<Vec<_>>(),
    );

    assert_eq!(0, tree.segment_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
