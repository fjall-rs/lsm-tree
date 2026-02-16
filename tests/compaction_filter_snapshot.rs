use lsm_tree::compaction::filter::{
    CompactionFilter, CompactionFilterContext, CompactionFilterFactory, ItemAccessor, Verdict,
};
use lsm_tree::{get_tmp_folder, AbstractTree, SeqNo, SequenceNumberCounter};

struct NukeFilter;

impl CompactionFilter for NukeFilter {
    fn filter_item(
        &mut self,
        _: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        // data? what data?
        Ok(Verdict::Remove)
    }
}

struct NukeFilterFactory;

impl CompactionFilterFactory for NukeFilterFactory {
    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(NukeFilter)
    }
}

#[test]
fn compaction_filter_snapshot() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let config = lsm_tree::Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .with_compaction_filter_factory(Some(Box::new(NukeFilterFactory)));
    let tree = config.open()?;

    tree.insert("a", "a", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", seqno.next());
    tree.flush_active_memtable(0)?;

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    tree.major_compact(u64::MAX, 0)?;

    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());
    assert!(tree.get("a", SeqNo::MAX)?.is_none());

    Ok(())
}
