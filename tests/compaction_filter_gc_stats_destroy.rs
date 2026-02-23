use lsm_tree::{
    blob_tree::FragmentationEntry,
    compaction::filter::{
        CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
    },
    get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

struct MyFilter;

impl CompactionFilter for MyFilter {
    fn filter_item(
        &mut self,
        item: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        if item.key() == b"removethis" {
            Ok(Verdict::Destroy)
        } else {
            Ok(Verdict::Keep)
        }
    }
}

struct MyFilterFactory;

impl Factory for MyFilterFactory {
    fn name(&self) -> &str {
        "Test"
    }

    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(MyFilter)
    }
}

#[test]
fn compaction_filter_gc_stats_destroy() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let other_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
        ))
        .with_compaction_filter_factory(Some(Arc::new(MyFilterFactory)))
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("removethis", &other_big_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "removethis":0 is dropped/destroyed
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = other_big_value.len() as u64;
                map.insert(0, FragmentationEntry::new(1, size, size));
                map
            },
            &*gc_stats,
        );
        assert_eq!(other_big_value.len() as u64, tree.stale_blob_bytes());
    }

    Ok(())
}
