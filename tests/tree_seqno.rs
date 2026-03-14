use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_highest_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert_eq!(tree.get_highest_seqno(), None);
    assert_eq!(tree.get_highest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("a", "a0", 0);
    assert_eq!(tree.get_highest_seqno(), Some(0));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(0));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("a", "a1", 1);
    assert_eq!(tree.get_highest_seqno(), Some(1));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(1));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("b", "b0", 2);
    assert_eq!(tree.get_highest_seqno(), Some(2));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(2));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.insert("b", "b1", 3);
    assert_eq!(tree.get_highest_seqno(), Some(3));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(3));
    assert_eq!(tree.get_highest_persisted_seqno(), None);

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get_highest_seqno(), Some(3));
    assert_eq!(tree.get_highest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    tree.insert("a", "a0", 4);
    assert_eq!(tree.get_highest_seqno(), Some(4));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(4));
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    tree.rotate_memtable().unwrap();

    assert_eq!(tree.get_highest_seqno(), Some(4));
    assert_eq!(tree.get_highest_memtable_seqno(), Some(4));
    assert_eq!(tree.get_highest_persisted_seqno(), Some(3));

    {
        let flush_lock = tree.get_flush_lock();
        assert!(tree.flush(&flush_lock, 0)?.unwrap() > 0);
    }

    assert_eq!(tree.get_highest_seqno(), Some(4));
    assert_eq!(tree.get_highest_memtable_seqno(), None);
    assert_eq!(tree.get_highest_persisted_seqno(), Some(4));

    Ok(())
}

#[test]
fn tree_highest_seqno_after_ingest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    // Write some items via normal insert so the seqno counter advances
    let s0 = seqno.next();
    tree.insert("x", "x0", s0);
    visible_seqno.fetch_max(s0 + 1);

    let s1 = seqno.next();
    tree.insert("y", "y0", s1);
    visible_seqno.fetch_max(s1 + 1);

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(s1));

    // Now bulk-ingest: items get local seqno 0 but the table carries
    // a global_seqno offset equal to the current seqno counter value.
    // seqno counter is at 2, so ingestion allocates global_seqno = 2.
    let mut ingestion = tree.ingestion()?;
    ingestion.write("a", "a0")?;
    ingestion.write("b", "b0")?;
    ingestion.finish()?;

    // global_seqno = 2, local max seqno = 0 → effective = 2
    let expected_seqno = seqno.get() - 1;

    // The persisted seqno must reflect the global offset, not raw local 0
    assert_eq!(
        tree.get_highest_persisted_seqno(),
        Some(expected_seqno),
        "ingested table must report effective seqno (global_seqno + local_seqno)"
    );

    // Overall highest must also include the ingested table
    assert_eq!(tree.get_highest_seqno(), Some(expected_seqno));

    // Verify data is visible at visible_seqno (which is > global_seqno)
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert!(tree.get("b", SeqNo::MAX)?.is_some());

    Ok(())
}
