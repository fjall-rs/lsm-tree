use lsm_tree::compaction::filter::{
    CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
};
use lsm_tree::{get_tmp_folder, AbstractTree, SeqNo, SequenceNumberCounter};
use std::sync::{Arc, Mutex};
use test_log::test;

/// Collects (key, seqno) pairs observed by the compaction filter.
struct SeqnoCollector(Arc<Mutex<Vec<(Vec<u8>, SeqNo)>>>);

impl CompactionFilter for SeqnoCollector {
    fn filter_item(
        &mut self,
        item: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        let key = item.key().to_vec();
        let seqno = item.seqno();
        self.0.lock().unwrap().push((key, seqno));
        Ok(Verdict::Keep)
    }
}

struct SeqnoCollectorFactory(Arc<Mutex<Vec<(Vec<u8>, SeqNo)>>>);

impl Factory for SeqnoCollectorFactory {
    fn name(&self) -> &str {
        "SeqnoCollector"
    }

    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(SeqnoCollector(self.0.clone()))
    }
}

#[test]
fn compaction_filter_seqno_matches_insert_time_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno_counter = SequenceNumberCounter::default();
    let collected: Arc<Mutex<Vec<(Vec<u8>, SeqNo)>>> = Arc::new(Mutex::new(Vec::new()));

    let config = lsm_tree::Config::new(
        &folder,
        seqno_counter.clone(),
        SequenceNumberCounter::default(),
    )
    .with_compaction_filter_factory(Some(Arc::new(SeqnoCollectorFactory(collected.clone()))));
    let tree = config.open()?;

    // Insert items with known seqnos
    let seqno_a = seqno_counter.next();
    tree.insert("a", "val_a", seqno_a);

    let seqno_b = seqno_counter.next();
    tree.insert("b", "val_b", seqno_b);

    let seqno_c = seqno_counter.next();
    tree.insert("c", "val_c", seqno_c);

    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    let observed = collected.lock().unwrap();

    // All three items should have been seen by the filter
    assert_eq!(observed.len(), 3, "filter should see all 3 items");

    // Verify seqno matches what was assigned at insert time
    let find = |key: &[u8]| observed.iter().find(|(k, _)| k == key).map(|(_, s)| *s);

    assert_eq!(find(b"a"), Some(seqno_a), "seqno mismatch for key 'a'");
    assert_eq!(find(b"b"), Some(seqno_b), "seqno mismatch for key 'b'");
    assert_eq!(find(b"c"), Some(seqno_c), "seqno mismatch for key 'c'");

    // Seqnos should be monotonically increasing
    assert!(seqno_a < seqno_b);
    assert!(seqno_b < seqno_c);

    Ok(())
}

#[test]
fn compaction_filter_seqno_below_cutoff_removes_item() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno_counter = SequenceNumberCounter::default();

    // Retention cutoff: items with seqno < cutoff get removed
    let cutoff: Arc<Mutex<SeqNo>> = Arc::new(Mutex::new(0));

    struct RetentionFilter {
        cutoff: Arc<Mutex<SeqNo>>,
    }

    impl CompactionFilter for RetentionFilter {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &CompactionFilterContext,
        ) -> lsm_tree::Result<Verdict> {
            let threshold = *self.cutoff.lock().unwrap();
            if item.seqno() < threshold {
                Ok(Verdict::Remove)
            } else {
                Ok(Verdict::Keep)
            }
        }
    }

    struct RetentionFilterFactory(Arc<Mutex<SeqNo>>);

    impl Factory for RetentionFilterFactory {
        fn name(&self) -> &str {
            "RetentionFilter"
        }

        fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
            Box::new(RetentionFilter {
                cutoff: self.0.clone(),
            })
        }
    }

    let config = lsm_tree::Config::new(
        &folder,
        seqno_counter.clone(),
        SequenceNumberCounter::default(),
    )
    .with_compaction_filter_factory(Some(Arc::new(RetentionFilterFactory(cutoff.clone()))));
    let tree = config.open()?;

    let seqno_old = seqno_counter.next();
    tree.insert("old", "val_old", seqno_old);

    let seqno_new = seqno_counter.next();
    tree.insert("new", "val_new", seqno_new);
    assert!(seqno_old < seqno_new, "expected monotonic seqno allocation");

    tree.flush_active_memtable(0)?;

    // Set cutoff between old and new seqnos — only "old" should be removed
    *cutoff.lock().unwrap() = seqno_new;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert!(
        tree.get("old", SeqNo::MAX)?.is_none(),
        "old item should be removed by retention filter"
    );
    assert!(
        tree.get("new", SeqNo::MAX)?.is_some(),
        "new item should be kept by retention filter"
    );

    Ok(())
}
