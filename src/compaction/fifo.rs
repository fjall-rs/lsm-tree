// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    compaction::state::CompactionState, config::Config, table::Table, time::unix_timestamp,
    version::Version, HashSet, KvPair,
};

#[doc(hidden)]
pub const NAME: &str = "FifoCompaction";

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest table(s)
/// when the threshold is reached.
///
/// Newly flushed tables are moved from level 0 into the last level, and merged
/// with overlapping tables when necessary.
///
/// Additionally, a (lazy) TTL can be configured to drop old tables.
///
/// ###### Caution
///
/// Only use it for specific workloads where:
///
/// 1) You only want to store recent data (unimportant logs, ...)
/// 2) The key order of inserts is strictly monotonically increasing or decreasing
/// 3) You only insert new data (no updates/deletes)
#[derive(Clone)]
pub struct Strategy {
    /// Data set size limit in bytes
    pub limit: u64,

    /// TTL in seconds, will be disabled if 0 or None
    pub ttl_seconds: Option<u64>,
}

impl Strategy {
    /// Configures a new `Fifo` compaction strategy
    #[must_use]
    pub fn new(limit: u64, ttl_seconds: Option<u64>) -> Self {
        Self { limit, ttl_seconds }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn get_config(&self) -> Vec<KvPair> {
        vec![
            (
                crate::UserKey::from("fifo_limit"),
                crate::UserValue::from(self.limit.to_le_bytes()),
            ),
            (
                crate::UserKey::from("fifo_ttl"),
                crate::UserValue::from(if self.ttl_seconds.is_some() {
                    [1u8]
                } else {
                    [0u8]
                }),
            ),
            (
                crate::UserKey::from("fifo_ttl_seconds"),
                crate::UserValue::from(self.ttl_seconds.map(u64::to_le_bytes).unwrap_or_default()),
            ),
        ]
    }

    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        let first_level = version.l0();
        let Some(last_level_idx) = config.level_count.checked_sub(1) else {
            return Choice::DoNothing;
        };

        let Some(last_level) = version.level(usize::from(last_level_idx)) else {
            return Choice::DoNothing;
        };

        if !first_level.is_empty() {
            if version.level_is_busy(0, state.hidden_set())
                || version.level_is_busy(usize::from(last_level_idx), state.hidden_set())
            {
                return Choice::DoNothing;
            }

            assert!(first_level.is_disjoint(), "L0 needs to be disjoint");

            let mut table_ids = first_level.list_ids();
            let key_range = first_level.aggregate_key_range();
            let overlapping_table_ids: Vec<_> = last_level
                .iter()
                .flat_map(|run| run.get_overlapping(&key_range))
                .map(Table::id)
                .collect();

            table_ids.extend(&overlapping_table_ids);

            let input = CompactionInput {
                table_ids,
                dest_level: last_level_idx,
                canonical_level: last_level_idx,
                target_size: 256 * 1_024 * 1_024,
            };

            return if overlapping_table_ids.is_empty() {
                Choice::Move(input)
            } else {
                Choice::Merge(input)
            };
        }

        // Early return avoids unnecessary work and keeps FIFO a no-op when there is nothing to do.
        if last_level.is_empty()
            || version.level_is_busy(usize::from(last_level_idx), state.hidden_set())
        {
            return Choice::DoNothing;
        }

        assert!(last_level.is_disjoint(), "Lmax needs to be disjoint");

        // Account for both table file bytes and value-log (blob) bytes to enforce the true space limit.
        let db_size = last_level.size() + version.blob_files.on_disk_size();

        let mut ids_to_drop = HashSet::default();

        // Compute TTL cutoff once and perform a single pass to mark expired tables and
        // accumulate their sizes. Also collect non-expired tables for possible size-based drops.
        let ttl_cutoff = match self.ttl_seconds {
            Some(s) if s > 0 => Some(
                unix_timestamp()
                    .as_nanos()
                    .saturating_sub(u128::from(s) * 1_000_000_000u128),
            ),
            _ => None,
        };

        let mut ttl_dropped_bytes = 0u64;
        let mut alive = Vec::new();

        for table in last_level.iter().flat_map(|run| run.iter()) {
            let expired =
                ttl_cutoff.is_some_and(|cutoff| u128::from(table.metadata.created_at) <= cutoff);

            if expired {
                ids_to_drop.insert(table.id());
                let linked_blob_file_bytes = table.referenced_blob_bytes().unwrap_or_default();
                ttl_dropped_bytes += table.file_size() + linked_blob_file_bytes;
            } else {
                alive.push(table);
            }
        }

        // Subtract TTL-selected bytes to see if we're still over the limit.
        let size_after_ttl = db_size.saturating_sub(ttl_dropped_bytes);

        // If we still exceed the limit, drop additional oldest tables until within the limit.
        if size_after_ttl > self.limit {
            let overshoot = size_after_ttl - self.limit;

            let mut collected_bytes = 0;

            // Oldest-first list by creation time from the non-expired set.
            alive.sort_by_key(|t| t.metadata.created_at);

            for table in alive {
                if collected_bytes >= overshoot {
                    break;
                }

                ids_to_drop.insert(table.id());

                let linked_blob_file_bytes = table.referenced_blob_bytes().unwrap_or_default();
                collected_bytes += table.file_size() + linked_blob_file_bytes;
            }
        }

        if ids_to_drop.is_empty() {
            Choice::DoNothing
        } else {
            Choice::Drop(ids_to_drop)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::{Arc, Mutex};

    static TIME_OVERRIDE_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn fifo_empty_levels() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let fifo = Arc::new(Strategy::new(1, None));
        tree.compact(fifo, 0)?;

        assert_eq!(0, tree.table_count());
        Ok(())
    }

    #[test]
    fn fifo_invalid_level_config_is_noop() -> crate::Result<()> {
        for level_count in [0, 8] {
            let dir = tempfile::tempdir()?;
            let mut config = Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            );
            config.level_count = level_count;
            let tree = config.open()?;

            tree.compact(Arc::new(Strategy::new(1, None)), 0)?;

            assert_eq!(0, tree.table_count());
        }
        Ok(())
    }

    #[test]
    fn fifo_below_limit() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0..4u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }

        let before = tree.table_count();
        let fifo = Arc::new(Strategy::new(u64::MAX, None));
        tree.compact(fifo, 4)?;

        assert_eq!(before, tree.table_count());
        Ok(())
    }

    #[test]
    fn fifo_more_than_limit() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0..4u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }

        let before = tree.table_count();
        // Very small limit forces dropping oldest tables
        let fifo = Arc::new(Strategy::new(1, None));
        tree.compact(fifo.clone(), 4)?;
        tree.compact(fifo, 4)?;

        assert!(tree.table_count() < before);
        Ok(())
    }

    #[test]
    fn fifo_more_than_limit_blobs() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "$", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }

        let before = tree.table_count();
        let fifo = Arc::new(Strategy::new(1, None));
        tree.compact(fifo.clone(), 3)?;
        tree.compact(fifo, 3)?;

        assert!(tree.table_count() < before);
        Ok(())
    }

    #[test]
    fn fifo_ttl() -> crate::Result<()> {
        let _time_lock = TIME_OVERRIDE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        // Freeze time and create first (older) table at t=1000s
        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_000)));
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;

        // Advance time and create second (newer) table at t=1005s
        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_005)));
        tree.insert("b", "2", 1);
        tree.flush_active_memtable(1)?;

        // Now set current time to t=1011s; with TTL=10s, cutoff=1001s => drop first only
        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_011)));

        assert_eq!(2, tree.table_count());

        let fifo = Arc::new(Strategy::new(u64::MAX, Some(10)));
        tree.compact(fifo.clone(), 2)?;
        tree.compact(fifo, 2)?;

        assert_eq!(1, tree.table_count());

        // Reset override
        crate::time::set_unix_timestamp_for_test(None);
        Ok(())
    }

    #[test]
    fn fifo_ttl_then_limit_additional_drops_blob_unit() -> crate::Result<()> {
        let _time_lock = TIME_OVERRIDE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_000)));
        tree.insert("a", "$", 0);
        tree.flush_active_memtable(0)?;

        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_005)));
        tree.insert("b", "$", 1);
        tree.flush_active_memtable(1)?;

        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(1_011)));

        // TTL drops the first table; the small limit drops the second.
        let fifo = Arc::new(Strategy::new(1, Some(10)));
        tree.compact(fifo.clone(), 2)?;
        tree.compact(fifo, 2)?;

        assert_eq!(0, tree.table_count());

        crate::time::set_unix_timestamp_for_test(None);
        Ok(())
    }

    #[test]
    fn fifo_drops_from_lmax_after_major_compaction() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0..4u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }

        tree.major_compact(u64::MAX, 4)?;
        let before = tree.table_count();
        tree.compact(Arc::new(Strategy::new(1, None)), 4)?;

        assert!(tree.table_count() < before);
        Ok(())
    }

    #[test]
    fn fifo_moves_disjoint_l0_tables_to_lmax() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;

        tree.insert("b", "2", 1);
        tree.flush_active_memtable(1)?;
        tree.compact(Arc::new(Strategy::new(u64::MAX, None)), 1)?;

        let version = tree.current_version();
        assert!(version.l0().is_empty());
        assert_eq!(
            Some(2),
            version
                .level(version.level_count() - 1)
                .map(|level| level.table_count())
        );
        Ok(())
    }

    #[test]
    fn fifo_merges_l0_with_overlapping_lmax_tables() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "1", 0);
        tree.insert("c", "3", 1);
        tree.flush_active_memtable(1)?;
        tree.major_compact(u64::MAX, 1)?;

        tree.insert("a", "1-updated", 2);
        tree.insert("b", "2", 2);
        tree.flush_active_memtable(2)?;
        tree.compact(Arc::new(Strategy::new(u64::MAX, None)), 2)?;

        let version = tree.current_version();
        assert!(version.l0().is_empty());
        assert_eq!(
            Some(1),
            version
                .level(version.level_count() - 1)
                .map(|level| level.table_count())
        );
        assert_eq!(b"2", &*tree.get("b", 3)?.expect("key should exist"));
        assert_eq!(b"1-updated", &*tree.get("a", 3)?.expect("key should exist"));
        Ok(())
    }
}
