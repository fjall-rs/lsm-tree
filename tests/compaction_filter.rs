use std::sync::{Arc, Mutex};
use std::u64;

use lsm_tree::compaction::filter::{CompactionFilter, FilterVerdict, ItemAccessor};
use lsm_tree::compaction::{CompactionOptions, PullDown};
use lsm_tree::{get_tmp_folder, AbstractTree, SeqNo, SequenceNumberCounter};

fn u32_s(n: u32) -> [u8; 4] {
    n.to_be_bytes()
}

fn u32_f(buf: &[u8]) -> u32 {
    let mut n = [0u8; 4];
    n.copy_from_slice(buf);
    u32::from_be_bytes(n)
}

#[test]
fn filter_basic() -> lsm_tree::Result<()> {
    struct FilterState {
        saw_value: bool,
        drop_threshold: u32,
    }

    static FILTER_STATE: Mutex<FilterState> = Mutex::new(FilterState {
        saw_value: false,
        drop_threshold: 4,
    });

    struct Filter;
    impl CompactionFilter for Filter {
        fn filter_item(&mut self, item: ItemAccessor<'_>) -> FilterVerdict {
            let mut state = FILTER_STATE.lock().unwrap();
            let key = u32_f(item.key());

            if key >= 0xff000000 {
                assert_eq!(key & 0xff, u32_f(&item.value().expect("failed fetch")));
                state.saw_value = true;
                FilterVerdict::Keep
            } else {
                if key < state.drop_threshold {
                    FilterVerdict::Drop
                } else {
                    FilterVerdict::Keep
                }
            }
        }
    }

    let folder = get_tmp_folder();

    let mut config = lsm_tree::Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()));
    config.level_count = 3;
    let tree = config.open()?;

    for i in 0u32..16 {
        tree.insert(u32_s(i), "old", 0);
    }

    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, CompactionOptions::from_seqno(SeqNo::MAX))?;

    for i in 0u32..16 {
        tree.insert(u32_s(i), "", 0);
        tree.insert(u32_s(i | 0xff000000), u32_s(i), 0);
    }

    tree.flush_active_memtable(0)?;

    tree.compact(
        Arc::new(PullDown(0, 1)),
        CompactionOptions::from_seqno(SeqNo::MAX).with_compaction_filter(Some(Box::new(Filter))),
    )?;

    // filter should have dropped this
    assert!(tree.get(u32_s(2), SeqNo::MAX)?.is_none());

    // but not these
    assert!(tree.get(u32_s(5), SeqNo::MAX)?.is_some());
    assert!(tree.get(u32_s(12), SeqNo::MAX)?.is_some());

    let mut state = FILTER_STATE.lock().unwrap();
    // filter should think it was called
    assert!(state.saw_value);
    state.saw_value = false;
    // up the threshold
    state.drop_threshold = 8;
    drop(state);

    // compact to last level
    tree.major_compact(
        u64::MAX,
        CompactionOptions::from_seqno(SeqNo::MAX).with_compaction_filter(Some(Box::new(Filter))),
    )?;

    // verify values gone
    assert!(tree.get(u32_s(2), SeqNo::MAX)?.is_none());
    assert!(tree.get(u32_s(5), SeqNo::MAX)?.is_none());
    // ensure the other value isn't
    assert!(tree.get(u32_s(12), SeqNo::MAX)?.is_some());

    Ok(())
}
