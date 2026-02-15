// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{
    compaction::state::CompactionState, config::Config, fs::FileSystem, time::unix_timestamp,
    version::Version, HashSet, KvPair,
};

#[doc(hidden)]
pub const NAME: &str = "FifoCompaction";

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest table(s)
/// when the threshold is reached.
///
/// Will also merge tables if the number of tables in level 0 grows too much, which
/// could cause write stalls.
///
/// Additionally, a (lazy) TTL can be configured to drop old tables.
///
/// ###### Caution
///
/// Only use it for specific workloads where:
///
/// 1) You only want to store recent data (unimportant logs, ...)
/// 2) Your keyspace grows monotonically (e.g. time series)
/// 3) You only insert new data (no updates)
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

impl<F: FileSystem> CompactionStrategy<F> for Strategy {
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

    fn choose(&self, version: &Version<F>, _: &Config<F>, state: &CompactionState) -> Choice {
        let first_level = version.l0();

        // Early return avoids unnecessary work and keeps FIFO a no-op when there is nothing to do.
        if first_level.is_empty() {
            return Choice::DoNothing;
        }

        assert!(first_level.is_disjoint(), "L0 needs to be disjoint");

        assert!(
            !version.level_is_busy(0, state.hidden_set()),
            "FIFO compaction never compacts",
        );

        // Account for both table file bytes and value-log (blob) bytes to enforce the true space limit.
        let db_size = first_level.size() + version.blob_files.on_disk_size();

        let mut ids_to_drop: HashSet<_> = HashSet::default();

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

        for table in first_level.iter().flat_map(|run| run.iter()) {
            let expired =
                ttl_cutoff.is_some_and(|cutoff| u128::from(table.metadata.created_at) <= cutoff);

            if expired {
                ids_to_drop.insert(table.id());
                let linked_blob_file_bytes = table.referenced_blob_bytes().unwrap_or_default();
                ttl_dropped_bytes =
                    ttl_dropped_bytes.saturating_add(table.file_size() + linked_blob_file_bytes);
            } else {
                alive.push(table);
            }
        }

        // Subtract TTL-selected bytes to see if we're still over the limit.
        let size_after_ttl = db_size.saturating_sub(ttl_dropped_bytes);

        // If we still exceed the limit, drop additional oldest tables until within the limit.
        if size_after_ttl > self.limit {
            let overshoot = size_after_ttl - self.limit;

            let mut collected_bytes = 0u64;

            // Oldest-first list by creation time from the non-expired set.
            alive.sort_by_key(|t| t.metadata.created_at);

            for table in alive {
                if collected_bytes >= overshoot {
                    break;
                }

                ids_to_drop.insert(table.id());

                let linked_blob_file_bytes = table.referenced_blob_bytes().unwrap_or_default();
                collected_bytes =
                    collected_bytes.saturating_add(table.file_size() + linked_blob_file_bytes);
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
    use std::sync::Arc;

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
        tree.compact(fifo, 3)?;

        assert!(tree.table_count() < before);
        Ok(())
    }

    #[test]
    fn fifo_ttl() -> crate::Result<()> {
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
        tree.compact(fifo, 2)?;

        assert_eq!(1, tree.table_count());

        // Reset override
        crate::time::set_unix_timestamp_for_test(None);
        Ok(())
    }

    #[test]
    fn fifo_ttl_then_limit_additional_drops_blob_unit() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

        // Create two tables; we will expire them via time override and force additional drops via limit.
        tree.insert("a", "$", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "$", 1);
        tree.flush_active_memtable(1)?;

        crate::time::set_unix_timestamp_for_test(Some(std::time::Duration::from_secs(10_000_000)));

        // TTL=1s will mark both expired; very small limit ensures size-based collection path is also exercised.
        let fifo = Arc::new(Strategy::new(1, Some(1)));
        tree.compact(fifo, 2)?;

        assert_eq!(0, tree.table_count());

        crate::time::set_unix_timestamp_for_test(None);
        Ok(())
    }
}
