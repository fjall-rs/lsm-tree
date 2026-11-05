//! Flush watermark: every item with a seqno at or below it is in a table,
//! or was removed on purpose (flush, compaction, drop range, clear).
//!
//! Every case checks the watermark in memory, and after recovering the tree
//! from the persisted version edits.

use lsm_tree::{
    compaction::{filter, Leveled, Major},
    AbstractTree, AnyTree, Config, EditRecord, IoVersionSetProvider, KvSeparationOptions,
    RawVersionData, SeqNo, VersionEdit, VersionEditPersister,
};
use std::sync::{Arc, Mutex};
use test_log::test;

/// Keeps all persisted version edits in memory, so they can be replayed
#[derive(Default)]
struct MemoryPersister(Mutex<Vec<Vec<EditRecord>>>);

impl VersionEditPersister for MemoryPersister {
    fn persist(&self, edit: &VersionEdit) -> lsm_tree::Result<()> {
        self.0
            .lock()
            .expect("lock is poisoned")
            .push(edit.iter().cloned().collect());
        Ok(())
    }
}

/// A tree that persists its version edits, so it can be recovered
struct RecoverableTree {
    dir: tempfile::TempDir,
    persister: Arc<MemoryPersister>,
    provider: Arc<IoVersionSetProvider>,
    tree: AnyTree,
    kv_opts: Option<KvSeparationOptions>,
}

impl RecoverableTree {
    fn new(kv_opts: Option<KvSeparationOptions>) -> lsm_tree::Result<Self> {
        Self::with_filter(kv_opts, None)
    }

    fn with_filter(
        kv_opts: Option<KvSeparationOptions>,
        filter: Option<Arc<dyn filter::Factory>>,
    ) -> lsm_tree::Result<Self> {
        let dir = tempfile::tempdir()?;

        let persister = Arc::new(MemoryPersister::default());
        let provider = Arc::new(IoVersionSetProvider::new(persister.clone()));
        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(kv_opts.clone())
            .with_compaction_filter_factory(filter)
            .open()?;

        Ok(Self {
            dir,
            persister,
            provider,
            tree,
            kv_opts,
        })
    }

    fn insert(&self, key: &str, seqno: SeqNo) {
        self.tree.insert(key, "value", seqno);
    }

    fn flush(&self, version_seqno: SeqNo) -> lsm_tree::Result<()> {
        self.provider
            .flush_active_memtable(&self.tree, version_seqno, 0)?;
        Ok(())
    }

    fn major_compact(&self, version_seqno: SeqNo) -> lsm_tree::Result<()> {
        self.provider
            .compact(&self.tree, Arc::new(Major::default()), version_seqno, 0)
    }

    /// Hydrates a fresh tree from the given raw version
    fn hydrate(&self, raw_state: &RawVersionData) -> lsm_tree::Result<AnyTree> {
        let provider = Arc::new(IoVersionSetProvider::new(Arc::new(
            MemoryPersister::default(),
        )));
        let tree = Config::new(self.dir.path(), provider.clone())
            .with_kv_separation(self.kv_opts.clone())
            .open()?;

        provider.hydrate(&tree, raw_state)?;

        Ok(tree)
    }

    /// Replays the persisted edits (only the records that pass `filter_record`),
    /// and hydrates a fresh tree from them
    fn recover_with(
        &self,
        filter_record: impl Fn(&EditRecord) -> bool,
    ) -> lsm_tree::Result<AnyTree> {
        let mut raw_state = RawVersionData::default();

        for edit in self.persister.0.lock().expect("lock is poisoned").iter() {
            raw_state.apply(&VersionEdit::new(
                edit.iter().filter(|r| filter_record(r)).cloned().collect(),
            ));
        }

        self.hydrate(&raw_state)
    }

    fn recover(&self) -> lsm_tree::Result<AnyTree> {
        self.recover_with(|_| true)
    }

    /// Checks the flush watermark in memory, and after recovering the tree
    fn assert_watermark(&self, expected: Option<SeqNo>) -> lsm_tree::Result<()> {
        assert_eq!(expected, self.tree.get_flush_watermark(), "in memory");
        assert_eq!(expected, self.recover()?.get_flush_watermark(), "recovered");
        Ok(())
    }
}

struct RemoveEverything;

impl filter::CompactionFilter for RemoveEverything {
    fn filter_item(
        &mut self,
        _item: filter::ItemAccessor<'_>,
        _ctx: &filter::Context,
    ) -> lsm_tree::Result<filter::Verdict> {
        Ok(filter::Verdict::Remove)
    }
}

struct RemoveEverythingFactory;

impl filter::Factory for RemoveEverythingFactory {
    fn name(&self) -> &str {
        "RemoveEverything"
    }

    fn make_filter(&self, _ctx: &filter::Context) -> Box<dyn filter::CompactionFilter> {
        Box::new(RemoveEverything)
    }
}

fn kv_variants() -> [Option<KvSeparationOptions>; 2] {
    [
        None,
        Some(KvSeparationOptions::default().separation_threshold(1)),
    ]
}

#[test]
fn flush_watermark_fresh_tree_and_seqno_zero() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        // Nothing flushed yet
        t.assert_watermark(None)?;

        // An unflushed item does not count
        t.insert("a", 0);
        assert_eq!(None, t.tree.get_flush_watermark());

        // The only flushed item has seqno 0, so the watermark is Some(0), not None
        t.flush(1)?;
        t.assert_watermark(Some(0))?;
    }

    Ok(())
}

#[test]
fn flush_watermark_is_highest_flushed_item_seqno() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        // a0..a9 = seqnos 0..=9, flush = 10
        for seqno in 0..=9 {
            t.insert(&format!("a{seqno}"), seqno);
        }
        t.flush(10)?;

        // NOTE: The highest flushed item, not the flush's version seqno
        t.assert_watermark(Some(9))?;

        // b0..b9 = seqnos 11..=20, flush = 21
        for seqno in 11..=20 {
            t.insert(&format!("b{seqno}"), seqno);
        }
        t.flush(21)?;
        t.assert_watermark(Some(20))?;

        // An unflushed item above the watermark does not count
        t.insert("c", 22);
        t.assert_watermark(Some(20))?;
    }

    Ok(())
}

/// A flush can drop its newest items (a weak tombstone cancels the value it removes),
/// so the highest table seqno may be lower than the highest flushed seqno
///
/// Otherwise:
/// 1. flush `a` = v1, then write `b`, `a` = v2, remove_weak(`a`)
/// 2. flush: v2 and the weak tombstone cancel out, the table only has `b`
/// 3. recover: the WAL replays the weak tombstone (above `b`), which hides v1
///
/// This would fail in v3.
#[test]
fn flush_watermark_includes_items_dropped_by_flush() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        t.insert("a", 0);
        t.flush(1)?;

        t.insert("b", 2);
        t.insert("a", 3);
        t.tree.remove_weak("a", 4);

        // NOTE: GC watermark above every item, so the flush may drop shadowed items
        t.provider.flush_active_memtable(&t.tree, 5, 1_000)?;

        assert!(
            t.tree.get("a", SeqNo::MAX)?.is_some(),
            "v1 should be visible again",
        );

        // NOTE: Every item up to the weak tombstone was flushed (or dropped by the flush)
        t.assert_watermark(Some(4))?;
    }

    Ok(())
}

#[test]
fn flush_watermark_kept_by_drop_range() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        for seqno in 0..=9 {
            t.insert(&format!("a{seqno}"), seqno);
        }
        t.flush(10)?;

        for seqno in 11..=20 {
            t.insert(&format!("b{seqno}"), seqno);
        }
        t.flush(21)?;
        assert_eq!(2, t.tree.table_count());

        // Dropping the table with the highest seqnos keeps the watermark,
        // otherwise the dropped items would be replayed again
        t.provider.drop_range(&t.tree, "b".."c", 22, 0)?;
        assert_eq!(1, t.tree.table_count());
        t.assert_watermark(Some(20))?;

        // An unflushed item, then dropping all tables: still the last flushed item,
        // NOT the drop's version seqno (23), which would skip the unflushed item
        t.insert("c", 23);
        t.provider.drop_range::<&str, _>(&t.tree, .., 24, 0)?;
        assert_eq!(0, t.tree.table_count());
        assert_eq!(Some(20), t.tree.current_version().flush_watermark());
        t.assert_watermark(Some(20))?;
    }

    Ok(())
}

#[test]
fn flush_watermark_kept_by_compactions() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        t.insert("a", 0);
        t.flush(1)?;

        // Trivial move of the only table
        t.provider
            .compact(&t.tree, Arc::new(Leveled::default()), 2, 0)?;
        t.assert_watermark(Some(0))?;

        t.insert("a", 3);
        t.insert("b", 4);
        t.flush(5)?;
        t.assert_watermark(Some(4))?;

        t.major_compact(6)?;
        assert_eq!(1, t.tree.table_count());
        t.assert_watermark(Some(4))?;
    }

    Ok(())
}

#[test]
fn flush_watermark_kept_when_filter_removes_everything() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::with_filter(kv_opts, Some(Arc::new(RemoveEverythingFactory)))?;

        for seqno in 0..=9 {
            t.insert(&format!("a{seqno}"), seqno);
        }
        t.flush(10)?;
        t.assert_watermark(Some(9))?;

        // NOTE: Not flushed, so the filter can't touch it
        t.insert("unflushed", 11);

        t.major_compact(12)?;

        // The filter removed every item, so there are no tables left; the watermark must NOT
        // become None (that would replay the removed items from a WAL), nor the
        // compaction's seqno (that would skip the unflushed item)
        assert_eq!(0, t.tree.table_count());
        assert_eq!(Some(9), t.tree.current_version().flush_watermark());
        t.assert_watermark(Some(9))?;

        assert!(11 > t.tree.get_flush_watermark().unwrap_or_default());
    }

    Ok(())
}

#[test]
fn flush_watermark_moved_up_by_clear() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        // Clear of a tree that never flushed anything
        t.provider.clear(&t.tree, 5)?;
        t.assert_watermark(Some(5))?;

        for seqno in 6..=19 {
            t.insert(&format!("a{seqno}"), seqno);
        }
        t.flush(20)?;
        t.assert_watermark(Some(19))?;

        // Clear of a tree with tables: the tree is empty afterwards,
        // but the watermark moves up to the clear's seqno
        t.provider.clear(&t.tree, 30)?;
        assert_eq!(0, t.tree.table_count());
        t.assert_watermark(Some(30))?;

        // An unflushed item after the clear does not count
        t.insert("b", 31);
        t.assert_watermark(Some(30))?;

        // Clear again, without flushing in between: moves up again
        t.provider.clear(&t.tree, 32)?;
        t.assert_watermark(Some(32))?;
    }

    Ok(())
}

#[test]
fn flush_watermark_never_decreases() -> lsm_tree::Result<()> {
    for kv_opts in kv_variants() {
        let t = RecoverableTree::new(kv_opts)?;

        let mut watermarks = vec![];

        t.insert("a", 0);
        t.insert("b", 1);
        t.flush(2)?;
        watermarks.push(t.tree.get_flush_watermark());

        t.insert("c", 3);
        t.flush(4)?;
        watermarks.push(t.tree.get_flush_watermark());

        t.major_compact(5)?;
        watermarks.push(t.tree.get_flush_watermark());

        t.provider.drop_range(&t.tree, "a"..="b", 6, 0)?;
        watermarks.push(t.tree.get_flush_watermark());

        t.provider.clear(&t.tree, 7)?;
        watermarks.push(t.tree.get_flush_watermark());

        t.insert("d", 8);
        t.flush(9)?;
        watermarks.push(t.tree.get_flush_watermark());

        t.provider.drop_range::<&str, _>(&t.tree, .., 10, 0)?;
        watermarks.push(t.tree.get_flush_watermark());

        assert_eq!(
            vec![
                Some(1),
                Some(3),
                Some(3),
                Some(3),
                Some(7),
                Some(8),
                Some(8)
            ],
            watermarks,
        );

        t.assert_watermark(Some(8))?;
    }

    Ok(())
}

#[test]
fn flush_watermark_survives_get_edit_records() -> lsm_tree::Result<()> {
    let t = RecoverableTree::new(None)?;

    t.insert("a", 0);
    t.flush(1)?;
    t.insert("b", 2);
    t.flush(3)?;
    t.major_compact(4)?;

    let mut raw_state = RawVersionData::default();
    raw_state.apply(&VersionEdit::new(t.provider.get_edit_records()));

    let tree = t.hydrate(&raw_state)?;

    assert_eq!(Some(2), tree.current_version().flush_watermark());
    assert_eq!(Some(2), tree.get_flush_watermark());

    Ok(())
}

// v3 versions have no flush watermark record, so the tables' highest seqno is used
#[test]
fn flush_watermark_falls_back_to_table_seqnos() -> lsm_tree::Result<()> {
    let t = RecoverableTree::new(None)?;

    t.insert("a", 0);
    t.insert("b", 1);
    t.flush(2)?;

    let tree = t.recover_with(|record| !matches!(record, EditRecord::SetFlushWatermark(_)))?;

    assert_eq!(None, tree.current_version().flush_watermark());
    assert_eq!(Some(1), tree.get_flush_watermark());

    Ok(())
}
