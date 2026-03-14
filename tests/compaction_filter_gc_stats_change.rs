use lsm_tree::SequenceNumberGenerator;
use lsm_tree::{
    blob_tree::FragmentationEntry,
    compaction::filter::{
        CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
    },
    get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn compaction_filter_gc_stats_change() -> lsm_tree::Result<()> {
    struct MyFilter;

    impl CompactionFilter for MyFilter {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &CompactionFilterContext,
        ) -> lsm_tree::Result<Verdict> {
            if item.key() == b"changethis" {
                Ok(Verdict::ReplaceValue(b"winter2".repeat(128_000).into()))
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

    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let other_big_value = b"asdasd".repeat(128_000);

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
        tree.insert("changethis", &other_big_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        // Because we are creating a new value, a new blob file is created
        assert_eq!(2, tree.blob_file_count());

        let value = tree.get("changethis", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"winter2".repeat(128_000));

        let gc_stats = tree.current_version().gc_stats().clone();

        // "changethis":0 was changed
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

#[test]
fn compaction_filter_gc_stats_change_non_blob() -> lsm_tree::Result<()> {
    struct MyFilter;

    impl CompactionFilter for MyFilter {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &CompactionFilterContext,
        ) -> lsm_tree::Result<Verdict> {
            if item.key() == b"changethis" {
                Ok(Verdict::ReplaceValue(b"winter2".into()))
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

    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let small_value = b"asdasd";

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
        tree.insert("changethis", small_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        // Because we are creating a new value, but a small one, a new blob file is not created
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("changethis", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"winter2");

        let gc_stats = tree.current_version().gc_stats().clone();

        // "changethis":0 was changed - but not a blob
        assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats,);
        assert_eq!(0, tree.stale_blob_bytes());
    }

    Ok(())
}

#[test]
fn compaction_filter_gc_stats_change_blob_writer_rotation() -> lsm_tree::Result<()> {
    struct MyFilter;

    impl CompactionFilter for MyFilter {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &CompactionFilterContext,
        ) -> lsm_tree::Result<Verdict> {
            if item.key().starts_with(b"changethis") {
                Ok(Verdict::ReplaceValue(b"winter2".repeat(128_000).into()))
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

    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let other_big_value = b"asdasd".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .file_target_size(1)
                .compression(lsm_tree::CompressionType::None),
        ))
        .with_compaction_filter_factory(Some(Arc::new(MyFilterFactory)))
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("changethis", &other_big_value, 0);
        tree.insert("changethis2", &other_big_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(3, tree.blob_file_count());

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        // Because we are creating a new value, a new blob file is created
        assert_eq!(5, tree.blob_file_count());

        let value = tree.get("changethis", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"winter2".repeat(128_000));

        let value = tree.get("changethis2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"winter2".repeat(128_000));

        let gc_stats = tree.current_version().gc_stats().clone();

        // "changethis":0,"changethis2":0 were changed
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = other_big_value.len() as u64;
                map.insert(1, FragmentationEntry::new(1, size, size));
                map.insert(2, FragmentationEntry::new(1, size, size));
                map
            },
            &*gc_stats,
        );
        assert_eq!(2 * other_big_value.len() as u64, tree.stale_blob_bytes());
    }

    Ok(())
}
