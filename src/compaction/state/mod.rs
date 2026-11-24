// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod hidden_set;

use crate::TableId;
use hidden_set::HiddenSet;
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Default)]
pub struct CompactionState {
    /// Set of table IDs that are masked.
    ///
    /// While consuming tables (because of compaction) they will not appear in the list of tables
    /// as to not cause conflicts between multiple compaction threads (compacting the same tables).
    hidden_set: HiddenSet,
}

impl CompactionState {
    pub fn hidden_set(&self) -> &HiddenSet {
        &self.hidden_set
    }

    pub fn hidden_set_mut(&mut self) -> &mut HiddenSet {
        &mut self.hidden_set
    }
}

/// Keeps tables hidden from other compactions until it is dropped
///
/// The guard is carried inside the compaction result, so the tables stay hidden
/// until the result has been committed (or discarded, e.g. because of an error).
///
/// IMPORTANT: Dropping the guard locks the compaction state, so it must not be dropped while
/// holding the compaction state lock, or the version set lock (lock order is compaction state -> version set).
#[must_use]
pub struct HiddenTablesGuard {
    compaction_state: Arc<Mutex<CompactionState>>,
    table_ids: Vec<TableId>,
}

impl HiddenTablesGuard {
    /// Hides the given tables.
    ///
    /// `state` is the locked compaction state that `compaction_state` points to.
    pub(crate) fn hide(
        state: &mut CompactionState,
        compaction_state: &Arc<Mutex<CompactionState>>,
        table_ids: impl IntoIterator<Item = TableId>,
    ) -> Self {
        let table_ids = table_ids.into_iter().collect::<Vec<_>>();

        state.hidden_set_mut().hide(table_ids.iter().copied());

        Self {
            compaction_state: compaction_state.clone(),
            table_ids,
        }
    }
}

impl Drop for HiddenTablesGuard {
    fn drop(&mut self) {
        // NOTE: Do not panic in Drop, we may already be unwinding
        self.compaction_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .hidden_set_mut()
            .show(self.table_ids.iter().copied());
    }
}

impl std::fmt::Debug for HiddenTablesGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HiddenTablesGuard({:?})", self.table_ids)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use crate::{
        compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict},
        AbstractTree, MemorySuperVersionSetProvider,
    };
    use std::sync::Arc;
    use test_log::test;

    struct FailingFilter;

    impl CompactionFilter for FailingFilter {
        fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> crate::Result<Verdict> {
            Err(crate::Error::Io(std::io::Error::other("injected")))
        }
    }

    struct FailingFactory;

    impl Factory for FailingFactory {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(FailingFilter)
        }
    }

    /// A failed compaction must show its input tables again
    #[test]
    fn compaction_error_unhides_tables() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let version_set_provider = Arc::new(MemorySuperVersionSetProvider::default());

        let tree = crate::Config::new(&folder, version_set_provider.clone())
            .with_compaction_filter_factory(Some(Arc::new(FailingFactory)))
            .open()?;

        for seqno in 0..3 {
            tree.insert("a", "a", seqno);
            version_set_provider.flush_active_memtable(&tree, seqno + 1, 0)?;
        }

        let table_count_before = tree.table_count();
        assert_eq!(3, table_count_before);

        let strategy = Arc::new(crate::compaction::major::Strategy::new(u64::MAX));
        assert!(version_set_provider
            .compact(&tree, strategy, 10, 0)
            .is_err());

        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!();
        };

        assert!(tree
            .compaction_state
            .lock()
            .expect("lock is poisoned")
            .hidden_set()
            .is_empty());

        assert_eq!(table_count_before, tree.table_count());

        Ok(())
    }

    /// Tables must stay hidden until the compaction result is committed,
    /// otherwise another compaction can pick the same input tables
    #[test]
    fn compaction_tables_hidden_until_commit() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let version_set_provider = Arc::new(MemorySuperVersionSetProvider::default());

        let tree = crate::Config::new(&folder, version_set_provider.clone()).open()?;

        for seqno in 0..3 {
            tree.insert("a", "a", seqno);
            version_set_provider.flush_active_memtable(&tree, seqno + 1, 0)?;
        }

        let strategy = Arc::new(crate::compaction::major::Strategy::new(u64::MAX));

        // Run compaction, but do not commit it yet
        let first = tree.run_compaction(strategy.clone(), 0)?;

        // Another compaction must not pick the (still uncommitted) input tables again
        let second = tree.run_compaction(strategy, 0)?;
        assert!(
            matches!(
                second.inner,
                crate::compaction::worker::CompactionOperation::Noop
            ),
            "second compaction picked the same input tables: {second:?}",
        );

        version_set_provider.register_compaction(first, 10)?;
        version_set_provider.register_compaction(second, 11)?;

        assert_eq!(1, tree.table_count());

        Ok(())
    }

    /// A compaction result that is committed after a clear must not bring back the cleared data
    #[test]
    fn compaction_after_clear_is_not_registered() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let version_set_provider = Arc::new(MemorySuperVersionSetProvider::default());

        let tree = crate::Config::new(&folder, version_set_provider.clone()).open()?;

        for seqno in 0..3 {
            tree.insert("a", "a", seqno);
            version_set_provider.flush_active_memtable(&tree, seqno + 1, 0)?;
        }

        let strategy = Arc::new(crate::compaction::major::Strategy::new(u64::MAX));

        // Run compaction, but commit it only after the clear
        let result = tree.run_compaction(strategy, 0)?;

        version_set_provider.clear(&tree, 10)?;
        version_set_provider.register_compaction(result, 11)?;

        assert_eq!(0, tree.table_count());
        assert!(tree.get("a", crate::SeqNo::MAX)?.is_none());

        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!();
        };

        assert!(tree
            .compaction_state
            .lock()
            .expect("lock is poisoned")
            .hidden_set()
            .is_empty());

        Ok(())
    }
}
