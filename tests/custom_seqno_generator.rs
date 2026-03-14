// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::{
    AbstractTree, Config, SeqNo, SequenceNumberCounter, SequenceNumberGenerator,
    SharedSequenceNumberGenerator,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Reserved MSB boundary — sequence numbers must stay below this.
const RESERVED_MSB: u64 = 0x8000_0000_0000_0000;

/// A custom generator that starts from a configurable offset,
/// proving the trait-object wiring works end-to-end.
///
/// Enforces the same MSB invariant as [`SequenceNumberCounter`].
#[derive(Debug)]
struct OffsetGenerator {
    counter: AtomicU64,
}

impl OffsetGenerator {
    fn new(start: u64) -> SharedSequenceNumberGenerator {
        assert!(start < RESERVED_MSB, "start must be below reserved MSB");
        Arc::new(Self {
            counter: AtomicU64::new(start),
        })
    }
}

impl SequenceNumberGenerator for OffsetGenerator {
    fn next(&self) -> SeqNo {
        match self
            .counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current >= RESERVED_MSB - 1 {
                    None
                } else {
                    Some(current + 1)
                }
            }) {
            Ok(seqno) => seqno,
            Err(_) => panic!("Ran out of sequence numbers"),
        }
    }

    fn get(&self) -> SeqNo {
        self.counter.load(Ordering::Acquire)
    }

    fn set(&self, value: SeqNo) {
        assert!(value < RESERVED_MSB, "value must be below reserved MSB");
        self.counter.store(value, Ordering::Release);
    }

    fn fetch_max(&self, value: SeqNo) {
        let clamped = value.min(RESERVED_MSB - 1);
        self.counter.fetch_max(clamped, Ordering::AcqRel);
    }
}

#[test]
fn custom_generator_via_builder() -> lsm_tree::Result<()> {
    let path = lsm_tree::get_tmp_folder();

    let seqno = OffsetGenerator::new(1000);
    let visible_seqno = OffsetGenerator::new(1000);

    let tree = Config::new(
        &path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
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
