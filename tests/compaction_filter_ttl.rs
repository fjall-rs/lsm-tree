use lsm_tree::{
    compaction::filter::{
        CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
    },
    AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc,
};
use test_log::test;

#[test]
fn compaction_filter_ttl() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    struct TtlFilter(u64);

    impl CompactionFilter for TtlFilter {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &CompactionFilterContext,
        ) -> lsm_tree::Result<Verdict> {
            let watermark_bytes = &self.0.to_be_bytes();

            if item.key() < watermark_bytes {
                Ok(Verdict::Destroy)
            } else {
                Ok(Verdict::Keep)
            }
        }
    }

    #[derive(Default)]
    struct TtlFilterFactory(Arc<AtomicU64>);

    impl Factory for TtlFilterFactory {
        fn name(&self) -> &str {
            "TTL"
        }

        fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
            Box::new(TtlFilter(self.0.load(Relaxed)))
        }
    }

    let watermark: Arc<AtomicU64> = Arc::default();
    let factory = TtlFilterFactory(watermark.clone());

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(folder, version_set_provider.clone())
        .with_compaction_filter_factory(Some(Arc::new(factory)))
        .open()?;

    tree.insert(1_000u64.to_be_bytes(), "my_value", seqno.next());
    tree.insert(1_001u64.to_be_bytes(), "my_value2", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.len(seqno.get(), None)?);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        100_000,
    )?;

    assert_eq!(2, tree.len(seqno.get(), None)?);

    watermark.store(1_000, Relaxed);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        100_000,
    )?;

    assert_eq!(2, tree.len(seqno.get(), None)?);

    watermark.store(1_001, Relaxed);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        100_000,
    )?;

    assert_eq!(1, tree.len(seqno.get(), None)?);

    Ok(())
}
