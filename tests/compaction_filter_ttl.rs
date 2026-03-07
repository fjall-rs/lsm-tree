use lsm_tree::{
    compaction::filter::{
        CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
    },
    AbstractTree, Config,
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

    let tree = Config::new(folder, Default::default(), Default::default())
        .with_compaction_filter_factory(Some(Arc::new(factory)))
        .open()?;

    tree.insert(1_000u64.to_be_bytes(), "my_value", 0);
    tree.insert(1_001u64.to_be_bytes(), "my_value2", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.len(100_000, None)?);

    tree.major_compact(64_000_000, 100_000)?;
    assert_eq!(2, tree.len(100_000, None)?);

    watermark.store(1_000, Relaxed);
    tree.major_compact(64_000_000, 100_000)?;
    assert_eq!(2, tree.len(100_000, None)?);

    watermark.store(1_001, Relaxed);
    tree.major_compact(64_000_000, 100_000)?;
    assert_eq!(1, tree.len(100_000, None)?);

    Ok(())
}
