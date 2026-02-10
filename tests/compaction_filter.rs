use lsm_tree::compaction::filter::{
    CompactionFilter, CompactionFilterFactory, FilterVerdict, ItemAccessor,
};
use lsm_tree::compaction::PullDown;
use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use std::sync::{Arc, Mutex};

fn u32_s(n: u32) -> [u8; 4] {
    n.to_be_bytes()
}

fn u32_f(buf: &[u8]) -> u32 {
    let mut n = [0u8; 4];
    n.copy_from_slice(buf);
    u32::from_be_bytes(n)
}

fn filter_basic(blob: bool) -> lsm_tree::Result<()> {
    struct FilterState {
        expect_blob: bool,
        disable: bool,
        do_rewrite: bool,
        saw_value: bool,
        drop_threshold: u32,
        finished: bool,
    }

    let filter_state = Arc::new(Mutex::new(FilterState {
        expect_blob: true,
        disable: true,
        do_rewrite: false,
        saw_value: false,
        drop_threshold: 4,
        finished: false,
    }));

    struct Filter(Arc<Mutex<FilterState>>);

    impl CompactionFilter for Filter {
        fn filter_item(&mut self, item: ItemAccessor<'_>) -> lsm_tree::Result<FilterVerdict> {
            let mut state = self.0.lock().unwrap();
            if state.disable {
                return Ok(FilterVerdict::Keep);
            }

            let key = u32_f(item.key());

            if key == 0x00abcdef {
                Ok(FilterVerdict::RemoveWeak)
            } else if key >= 0xff000000 {
                let value = u32_f(&item.value()?);
                assert_eq!(key & 0xff, value);
                assert_eq!(item.is_indirection(), state.expect_blob);
                state.saw_value = true;

                if !state.do_rewrite {
                    Ok(FilterVerdict::Keep)
                } else {
                    Ok(FilterVerdict::ReplaceValue(
                        vec![b'a'; value as usize].into(),
                    ))
                }
            } else {
                assert_eq!(item.value().expect("failed fetch"), b"a");
                if key < state.drop_threshold {
                    Ok(FilterVerdict::Remove)
                } else {
                    Ok(FilterVerdict::Keep)
                }
            }
        }

        fn finish(self: Box<Self>) {
            let mut state = self.0.lock().unwrap();
            state.finished = true;
        }
    }

    struct FilterFactory(Arc<Mutex<FilterState>>);

    impl CompactionFilterFactory for FilterFactory {
        fn make_filter(&self) -> Box<dyn CompactionFilter> {
            Box::new(Filter(self.0.clone()))
        }
    }

    let folder = get_tmp_folder();

    let mut config = lsm_tree::Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(if blob {
        Some(KvSeparationOptions::default().separation_threshold(2))
    } else {
        None
    })
    .with_compaction_filter_factory(Some(Box::new(FilterFactory(filter_state.clone()))));
    config.level_count = 3;
    let tree = config.open()?;

    for i in 0u32..255 {
        tree.insert(u32_s(i), "old", 0);
    }

    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    for i in 0u32..255 {
        tree.insert(u32_s(i), "a", 0);
        tree.insert(u32_s(i | 0xff000000), u32_s(i), 0);
    }

    tree.insert(u32_s(0x00abcdef), "test", 0);

    tree.flush_active_memtable(0)?;

    let mut state = filter_state.lock().unwrap();
    state.disable = false;
    state.expect_blob = blob;
    drop(state);

    tree.compact(Arc::new(PullDown(0, 1)), SeqNo::MAX)?;

    // filter should have dropped this
    assert!(tree.get(u32_s(2), SeqNo::MAX)?.is_none());

    // but not these
    assert!(tree.get(u32_s(5), SeqNo::MAX)?.is_some());
    assert!(tree.get(u32_s(12), SeqNo::MAX)?.is_some());

    // ensure weak-deleted value is gone
    assert!(tree.get(u32_s(0x00abcdef), SeqNo::MAX)?.is_none());

    let mut state = filter_state.lock().unwrap();
    // filter should think it was called
    assert!(state.saw_value);
    assert!(state.finished);
    state.saw_value = false;
    // up the threshold
    state.drop_threshold = 8;
    state.do_rewrite = true;
    drop(state);

    // compact to last level
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    // verify values gone
    assert!(tree.get(u32_s(2), SeqNo::MAX)?.is_none());
    assert!(tree.get(u32_s(5), SeqNo::MAX)?.is_none());
    // ensure the other value isn't
    assert!(tree.get(u32_s(12), SeqNo::MAX)?.is_some());

    // verify rewrites work
    assert_eq!(tree.get(u32_s(2 | 0xff000000), SeqNo::MAX)?.unwrap(), b"aa");
    assert_eq!(
        tree.get(u32_s(37 | 0xff000000), SeqNo::MAX)?.unwrap(),
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );

    let state = filter_state.lock().unwrap();
    assert!(state.saw_value);
    drop(state);

    Ok(())
}

#[test]
fn filter_with_blob() -> lsm_tree::Result<()> {
    filter_basic(true)
}

#[test]
fn filter_no_blob() -> lsm_tree::Result<()> {
    filter_basic(false)
}

#[test]
fn filter_snapshot() -> lsm_tree::Result<()> {
    struct DropEverything;

    impl CompactionFilter for DropEverything {
        fn filter_item(&mut self, _: ItemAccessor<'_>) -> lsm_tree::Result<FilterVerdict> {
            // data? what data?
            Ok(FilterVerdict::Remove)
        }
    }

    struct DropEverythingFactory;

    impl CompactionFilterFactory for DropEverythingFactory {
        fn make_filter(&self) -> Box<dyn CompactionFilter> {
            Box::new(DropEverything)
        }
    }

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let config = lsm_tree::Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .with_compaction_filter_factory(Some(Box::new(DropEverythingFactory)));
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
