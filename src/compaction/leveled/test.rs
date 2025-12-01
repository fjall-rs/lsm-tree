mod tests {
    use super::{Choice, Strategy};
    use crate::{
        cache::Cache,
        compaction::{CompactionStrategy, Input as CompactionInput},
        descriptor_table::FileDescriptorTable,
        level_manifest::LevelManifest,
        segment::{
            block::offset::BlockOffset,
            block_index::{two_level_index::TwoLevelBlockIndex, BlockIndexImpl},
            file_offsets::FileOffsets,
            meta::{Metadata, SegmentId},
            SegmentInner,
        },
        super_segment::Segment,
        time::unix_timestamp,
        Config, HashSet, KeyRange,
    };
    use std::{
        path::Path,
        sync::{atomic::AtomicBool, Arc},
    };
    use test_log::test;

    fn string_key_range(a: &str, b: &str) -> KeyRange {
        KeyRange::new((a.as_bytes().into(), b.as_bytes().into()))
    }

    #[allow(
        clippy::expect_used,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn fixture_segment(
        id: SegmentId,
        key_range: KeyRange,
        size: u64,
        tombstone_ratio: f32,
    ) -> Segment {
        todo!()

        /*   let cache = Arc::new(Cache::with_capacity_bytes(10 * 1_024 * 1_024));

        let block_index = TwoLevelBlockIndex::new((0, id).into(), cache.clone());
        let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

        SegmentInner {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index,

            offsets: FileOffsets {
                bloom_ptr: BlockOffset(0),
                range_filter_ptr: BlockOffset(0),
                index_block_ptr: BlockOffset(0),
                metadata_ptr: BlockOffset(0),
                range_tombstones_ptr: BlockOffset(0),
                tli_ptr: BlockOffset(0),
                pfx_ptr: BlockOffset(0),
            },

            metadata: Metadata {
                data_block_count: 0,
                index_block_count: 0,
                data_block_size: 4_096,
                index_block_size: 4_096,
                created_at: unix_timestamp().as_nanos(),
                id,
                file_size: size,
                compression: crate::segment::meta::CompressionType::None,
                table_type: crate::segment::meta::TableType::Block,
                item_count: 1_000_000,
                key_count: 0,
                key_range,
                tombstone_count: (1_000_000.0 * tombstone_ratio) as u64,
                range_tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            cache,

            bloom_filter: Some(crate::bloom::BloomFilter::with_fp_rate(1, 0.1)),

            path: "a".into(),
            is_deleted: AtomicBool::default(),
        }
        .into() */
    }

    #[allow(clippy::expect_used)]
    fn build_levels(
        path: &Path,
        recipe: Vec<Vec<(SegmentId, &str, &str, u64)>>,
    ) -> crate::Result<LevelManifest> {
        let mut levels = LevelManifest::create_new(
            recipe.len().try_into().expect("oopsie"),
            path.join("levels"),
        )?;

        for (idx, level) in recipe.into_iter().enumerate() {
            for (id, min, max, size_mib) in level {
                levels.insert_into_level(
                    idx.try_into().expect("oopsie"),
                    fixture_segment(
                        id,
                        string_key_range(min, max),
                        size_mib * 1_024 * 1_024,
                        0.0,
                    ),
                );
            }
        }

        Ok(levels)
    }

    #[test]
    fn leveled_empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn leveled_default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let mut levels = build_levels(tempdir.path(), vec![
            vec![(1, "a", "z", 64), (2, "a", "z", 64), (3, "a", "z", 64), (4, "a", "z", 64)],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        levels.hide_segments(std::iter::once(4));

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation
    )]
    fn leveled_intra_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let mut levels = build_levels(tempdir.path(), vec![
            vec![(1, "a", "z", 1), (2, "a", "z", 1), (3, "a", "z", 1), (4, "a", "z", 1)],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 0,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
                target_size: u64::from(compactor.target_size),
            })
        );

        levels.hide_segments(std::iter::once(4));

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn leveled_more_than_min_no_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![(1, "h", "t", 64), (2, "h", "t", 64), (3, "h", "t", 64), (4, "h", "t", 64)],
            vec![(5, "a", "g", 64), (6, "a", "g", 64), (7, "a", "g", 64), (8, "a", "g", 64)],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn leveled_more_than_min_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let mut levels = build_levels(tempdir.path(), vec![
            vec![(1, "a", "g", 64), (2, "h", "t", 64), (3, "i", "t", 64), (4, "j", "t", 64)],
            vec![(5, "a", "g", 64), (6, "a", "g", 64), (7, "y", "z", 64), (8, "y", "z", 64)],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: [1, 2, 3, 4, 5, 6].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        levels.hide_segments(std::iter::once(5));
        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn levelled_from_tiered() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![(1, "a", "z", 64), (2, "a", "z", 64), (3, "g", "z", 64), (5, "g", "z", 64), (6, "g", "z", 64)],
            vec![(4, "a", "g", 64)],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: [1, 2, 3, 4, 5, 6].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }
}
