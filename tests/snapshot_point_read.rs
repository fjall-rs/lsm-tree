use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn snapshot_404() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("a2", "a2", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable(0)?;

    assert_eq!(b"a", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert_eq!(b"a2", &*tree.get("a2", SeqNo::MAX)?.unwrap());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert_eq!(b"c", &*tree.get("c", SeqNo::MAX)?.unwrap());

    assert!(tree.get("a", 0)?.is_none());
    assert!(tree.get("a2", 0)?.is_none());
    assert!(tree.get("b", 0)?.is_none());
    assert!(tree.get("c", 0)?.is_none());

    assert_eq!(b"a", &*tree.get("a", 1)?.unwrap());
    assert_eq!(b"a2", &*tree.get("a2", 1)?.unwrap());
    assert!(tree.get("b", 1)?.is_none());
    assert_eq!(b"c", &*tree.get("c", 1)?.unwrap());

    Ok(())
}

#[test]
fn snapshot_lots_of_versions() -> lsm_tree::Result<()> {
    let version_count = 600;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    let key = "abc";

    let seqno = SequenceNumberCounter::default();

    #[allow(clippy::explicit_counter_loop)]
    for _ in 0u64..version_count {
        tree.insert(key, format!("abc{version_count}").as_bytes(), seqno.next());
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);

    for seqno in 1..version_count {
        let item = tree
            .get_internal_entry(key.as_bytes(), seqno)?
            .expect("should exist");

        assert_eq!(format!("abc{}", version_count).as_bytes(), &*item.value);

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!(format!("abc{}", version_count).as_bytes(), &*item);
    }

    Ok(())
}

const ITEM_COUNT: usize = 1;
const BATCHES: usize = 10;

#[test]
fn snapshot_disk_point_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    let seqno = SequenceNumberCounter::default();

    for batch in 0..BATCHES {
        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("abc{batch}").as_bytes(), seqno.next());
        }
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);
    }

    let snapshot_seqno = seqno.get();

    assert_eq!(tree.len(SeqNo::MAX, None)?, tree.len(snapshot_seqno, None)?);

    // This batch will be too new for snapshot (invisible)
    for batch in 0..BATCHES {
        let batch_seqno = seqno.next();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("def{batch}").as_bytes(), batch_seqno);
        }
    }
    tree.flush_active_memtable(0)?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key, snapshot_seqno)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!("def9".as_bytes(), &*item);
    }

    Ok(())
}

#[test]
fn snapshot_disk_and_memtable_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    let seqno = SequenceNumberCounter::default();

    for batch in 0..BATCHES {
        let batch_seqno = seqno.next();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("abc{batch}").as_bytes(), batch_seqno);
        }
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    let snapshot_seqno = seqno.get();

    assert_eq!(tree.len(SeqNo::MAX, None)?, tree.len(snapshot_seqno, None)?);

    // This batch will be in memtable and too new for snapshot (invisible)
    for batch in 0..BATCHES {
        let batch_seqno = seqno.next();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("def{batch}").as_bytes(), batch_seqno);
        }
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key, snapshot_seqno)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!("def9".as_bytes(), &*item);
    }

    Ok(())
}
