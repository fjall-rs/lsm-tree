// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::{
    AbstractTree, Config, SeqNo, SequenceNumberGenerator, SharedSequenceNumberGenerator,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A custom generator that starts from a configurable offset,
/// proving the trait-object wiring works end-to-end.
#[derive(Debug)]
struct OffsetGenerator {
    counter: AtomicU64,
}

impl OffsetGenerator {
    fn new(start: u64) -> SharedSequenceNumberGenerator {
        Arc::new(Self {
            counter: AtomicU64::new(start),
        })
    }
}

impl SequenceNumberGenerator for OffsetGenerator {
    fn next(&self) -> SeqNo {
        self.counter.fetch_add(1, Ordering::AcqRel)
    }

    fn get(&self) -> SeqNo {
        self.counter.load(Ordering::Acquire)
    }

    fn set(&self, value: SeqNo) {
        self.counter.store(value, Ordering::Release);
    }

    fn fetch_max(&self, value: SeqNo) {
        self.counter.fetch_max(value, Ordering::AcqRel);
    }
}

#[test]
fn custom_generator_via_builder() -> lsm_tree::Result<()> {
    let path = lsm_tree::get_tmp_folder();

    let seqno = OffsetGenerator::new(1000);
    let visible_seqno = OffsetGenerator::new(1000);

    let tree = Config::new_with_generators(&path, seqno.clone(), visible_seqno.clone())
        .seqno_generator(seqno.clone())
        .visible_seqno_generator(visible_seqno.clone())
        .open()?;

    let s = seqno.next();
    assert_eq!(s, 1000);
    tree.insert(b"key", b"value", s);
    visible_seqno.fetch_max(s + 1);

    assert_eq!(tree.len(visible_seqno.get(), None)?, 1);

    Ok(())
}

#[test]
fn custom_generator_via_new_with_generators() -> lsm_tree::Result<()> {
    let path = lsm_tree::get_tmp_folder();

    let seqno = OffsetGenerator::new(5000);
    let visible_seqno = OffsetGenerator::new(5000);

    let tree = Config::new_with_generators(&path, seqno.clone(), visible_seqno.clone()).open()?;

    let s = seqno.next();
    assert_eq!(s, 5000);
    tree.insert(b"hello", b"world", s);
    visible_seqno.fetch_max(s + 1);

    let val = tree.get(b"hello", visible_seqno.get())?.unwrap();
    assert_eq!(&*val, b"world");

    Ok(())
}
