use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{levels::LevelManifest, Config};

fn desired_level_size_in_bytes(level_idx: u8, ratio: u8, base_size: u32) -> usize {
    (ratio as usize).pow(u32::from(level_idx + 1)) * (base_size as usize)
}

/// Size-tiered compaction strategy (STCS)
///
/// If a level reaches a threshold, it is merged into a larger segment to the next level.
///
/// STCS suffers from high read and temporary space amplification, but decent write amplification.
///
/// More info here: <https://opensource.docs.scylladb.com/stable/cql/compaction.html#size-tiered-compaction-strategy-stcs>
pub struct Strategy {
    base_size: u32,
}

impl Strategy {
    /// Creates a new STCS strategy with custom base size
    #[must_use]
    pub fn new(base_size: u32) -> Self {
        Self { base_size }
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            base_size: 8 * 1_024 * 1_024,
        }
    }
}

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &LevelManifest, config: &Config) -> Choice {
        let resolved_view = levels.resolved_view();

        for (curr_level_index, level) in resolved_view
            .iter()
            .enumerate()
            .map(|(idx, lvl)| (idx as u8, lvl))
            .take(resolved_view.len() - 1)
            .rev()
        {
            let next_level_index = curr_level_index + 1;

            if level.is_empty() {
                continue;
            }

            let curr_level_bytes = level.size();

            let desired_bytes =
                desired_level_size_in_bytes(curr_level_index, config.level_ratio, self.base_size)
                    as u64;

            if curr_level_bytes >= desired_bytes {
                // NOTE: Take desired_bytes because we are in tiered mode
                // We want to take N segments, not just the overshoot (like in levelled)
                let mut overshoot = desired_bytes as usize;

                let mut segments_to_compact = vec![];

                for segment in level.iter().rev().take(config.level_ratio.into()).cloned() {
                    if overshoot == 0 {
                        break;
                    }

                    overshoot = overshoot.saturating_sub(segment.metadata.file_size as usize);
                    segments_to_compact.push(segment);
                }

                let segment_ids: Vec<_> = segments_to_compact
                    .iter()
                    .map(|x| &x.metadata.id)
                    .copied()
                    .collect();

                return Choice::DoCompact(CompactionInput {
                    segment_ids,
                    dest_level: next_level_index,
                    target_size: u64::MAX,
                });
            }
        }

        // TODO: if level.size >= base_size and there are enough
        // segments with size < base_size, compact them together
        // no matter the amount of segments in L0 -> should reduce
        // write stall chance
        //
        // TODO: however: force compaction if L0 becomes way too large

        // NOTE: Reduce L0 segments if needed
        // this is probably an edge case if the `base_size` does not line up with
        // the `max_memtable_size` AT ALL
        super::maintenance::Strategy.choose(levels, config)
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        block_cache::BlockCache,
        compaction::{Choice, CompactionStrategy, Input as CompactionInput},
        config::Config,
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        key_range::KeyRange,
        levels::LevelManifest,
        segment::{
            block_index::BlockIndex,
            meta::{Metadata, SegmentId},
            Segment,
        },
        SeqNo,
    };
    use std::sync::Arc;
    use test_log::test;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: SegmentId, size_mib: u64, max_seqno: SeqNo) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        Arc::new(Segment {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(BlockIndex::new((0, id).into(), block_cache.clone())),
            metadata: Metadata {
                block_count: 0,
                block_size: 0,
                created_at: 0,
                id,
                file_size: size_mib * 1_024 * 1_024,
                compression: crate::segment::meta::CompressionType::Lz4,
                table_type: crate::segment::meta::TableType::Block,
                item_count: 0,
                key_count: 0,
                key_range: KeyRange::new((vec![].into(), vec![].into())),
                tombstone_count: 0,
                range_tombstone_count: 0,
                uncompressed_size: size_mib * 1_024 * 1_024,
                seqnos: (0, max_seqno),
            },
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[test]
    fn empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };

        let levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };
        let config = Config::default().level_ratio(4);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment(1, 8, 5));
        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        levels.add(fixture_segment(2, 8, 6));
        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        levels.add(fixture_segment(3, 8, 7));
        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        levels.add(fixture_segment(4, 8, 8));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec![1, 2, 3, 4],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn ordering() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };
        let config = Config::default().level_ratio(2);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment(1, 8, 0));
        levels.add(fixture_segment(2, 8, 1));
        levels.add(fixture_segment(3, 8, 2));
        levels.add(fixture_segment(4, 8, 3));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec![1, 2],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn more_than_min() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };
        let config = Config::default().level_ratio(4);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(1, 8, 5));
        levels.add(fixture_segment(2, 8, 6));
        levels.add(fixture_segment(3, 8, 7));
        levels.add(fixture_segment(4, 8, 8));

        levels.insert_into_level(1, fixture_segment(5, 8 * 4, 9));
        levels.insert_into_level(1, fixture_segment(6, 8 * 4, 10));
        levels.insert_into_level(1, fixture_segment(7, 8 * 4, 11));
        levels.insert_into_level(1, fixture_segment(8, 8 * 4, 12));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 2,
                segment_ids: vec![5, 6, 7, 8],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn many_segments() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };
        let config = Config::default().level_ratio(2);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(1, 8, 5));
        levels.add(fixture_segment(2, 8, 6));
        levels.add(fixture_segment(3, 8, 7));
        levels.add(fixture_segment(4, 8, 8));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec![1, 2],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn deeper_level() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };
        let config = Config::default().level_ratio(2);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(1, 8, 5));

        levels.insert_into_level(1, fixture_segment(2, 8 * 2, 6));
        levels.insert_into_level(1, fixture_segment(3, 8 * 2, 7));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 2,
                segment_ids: vec![2, 3],
                target_size: u64::MAX,
            })
        );

        let tempdir = tempfile::tempdir()?;
        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.insert_into_level(2, fixture_segment(2, 8 * 4, 5));
        levels.insert_into_level(2, fixture_segment(3, 8 * 4, 6));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 3,
                segment_ids: vec![2, 3],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn last_level() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            base_size: 8 * 1_024 * 1_024,
        };
        let config = Config::default().level_ratio(2);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.insert_into_level(3, fixture_segment(2, 8, 5));
        levels.insert_into_level(3, fixture_segment(3, 8, 5));

        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        Ok(())
    }
}
