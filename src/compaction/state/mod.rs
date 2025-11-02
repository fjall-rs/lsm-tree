// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod hidden_set;

use hidden_set::HiddenSet;

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

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use crate::{AbstractTree, SequenceNumberCounter};
    use test_log::test;

    #[test]
    #[ignore]
    fn level_manifest_atomicity() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(folder, SequenceNumberCounter::default()).open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 2);
        tree.flush_active_memtable(0)?;

        assert_eq!(3, tree.approximate_len());

        tree.major_compact(u64::MAX, 3)?;

        assert_eq!(1, tree.table_count());

        tree.insert("a", "a", 3);
        tree.flush_active_memtable(0)?;

        let table_count_before_major_compact = tree.table_count();

        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!();
        };

        // {
        //     // NOTE: Purposefully change level manifest to have invalid path
        //     // to force an I/O error
        //     tree.compaction_state
        //         .lock()
        //         .expect("lock is poisoned")
        //         .folder = "/invaliiid/asd".into();
        // }

        assert!(tree.major_compact(u64::MAX, 4).is_err());

        assert!(tree
            .compaction_state
            .lock()
            .expect("lock is poisoned")
            .hidden_set()
            .is_empty());

        assert_eq!(table_count_before_major_compact, tree.table_count());

        Ok(())
    }
}
