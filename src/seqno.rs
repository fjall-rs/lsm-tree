// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::SeqNo;
use std::{
    fmt::Debug,
    sync::{
        atomic::{
            AtomicU64,
            Ordering::{AcqRel, Acquire, Release},
        },
        Arc,
    },
};

/// Trait for custom sequence number generation.
///
/// Implementations must be thread-safe and provide atomic operations
/// for sequence number management.
///
/// The default implementation is [`SequenceNumberCounter`], which uses
/// an `Arc<AtomicU64>` for lock-free atomic operations.
pub trait SequenceNumberGenerator:
    Send + Sync + Debug + std::panic::UnwindSafe + std::panic::RefUnwindSafe + 'static
{
    /// Gets the next sequence number, atomically incrementing the counter.
    fn next(&self) -> SeqNo;

    /// Gets the current sequence number without incrementing.
    fn get(&self) -> SeqNo;

    /// Sets the sequence number to the given value.
    fn set(&self, value: SeqNo);

    /// Atomically updates the sequence number to the maximum of
    /// the current value and the given value.
    fn fetch_max(&self, value: SeqNo);
}

/// A shared, cloneable sequence number generator.
pub type SharedSequenceNumberGenerator = Arc<dyn SequenceNumberGenerator>;

/// Thread-safe sequence number generator
///
/// # Examples
///
/// ```
/// # use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
/// #
/// # let path = tempfile::tempdir()?;
///
/// let seqno = SequenceNumberCounter::default();
/// let visible_seqno = SequenceNumberCounter::default();
///
/// let tree = Config::new(path, seqno.clone(), visible_seqno.clone()).open()?;
///
/// // Do some inserts...
/// for k in [b"a", b"b", b"c"] {
///     let batch_seqno = seqno.next();
///     tree.insert(k, "abc", batch_seqno);
///     visible_seqno.fetch_max(batch_seqno + 1);
/// }
///
/// // Create a batch
/// let batch_seqno = seqno.next();
/// tree.remove("a".as_bytes(), batch_seqno);
/// tree.remove("b".as_bytes(), batch_seqno);
/// tree.remove("c".as_bytes(), batch_seqno);
/// visible_seqno.fetch_max(batch_seqno + 1);
/// #
/// # assert!(tree.is_empty(visible_seqno.get(), None)?);
/// # Ok::<(), lsm_tree::Error>(())
/// ```
#[derive(Clone, Default, Debug)]
pub struct SequenceNumberCounter(Arc<AtomicU64>);

impl SequenceNumberCounter {
    /// Creates a new counter, setting it to some previous value
    #[must_use]
    pub fn new(prev: SeqNo) -> Self {
        Self(Arc::new(AtomicU64::new(prev)))
    }

    /// Gets the next sequence number, atomically incrementing the counter.
    #[must_use]
    #[allow(clippy::missing_panics_doc, reason = "we should never run out of u64s")]
    pub fn next(&self) -> SeqNo {
        <Self as SequenceNumberGenerator>::next(self)
    }

    /// Gets the current sequence number without incrementing.
    #[must_use]
    pub fn get(&self) -> SeqNo {
        <Self as SequenceNumberGenerator>::get(self)
    }

    /// Sets the sequence number to the given value.
    pub fn set(&self, value: SeqNo) {
        <Self as SequenceNumberGenerator>::set(self, value)
    }

    /// Atomically updates the sequence number to the maximum of
    /// the current value and the provided value.
    pub fn fetch_max(&self, value: SeqNo) {
        <Self as SequenceNumberGenerator>::fetch_max(self, value)
    }
}

impl SequenceNumberGenerator for SequenceNumberCounter {
    #[allow(clippy::missing_panics_doc, reason = "we should never run out of u64s")]
    fn next(&self) -> SeqNo {
        let seqno = self.0.fetch_add(1, AcqRel);

        // The MSB is reserved for transactions.
        //
        // This gives us 63-bit sequence numbers technically.
        assert!(seqno < 0x8000_0000_0000_0000, "Ran out of sequence numbers");

        seqno
    }

    fn get(&self) -> SeqNo {
        self.0.load(Acquire)
    }

    fn set(&self, value: SeqNo) {
        self.0.store(value, Release);
    }

    fn fetch_max(&self, value: SeqNo) {
        self.0.fetch_max(value, AcqRel);
    }
}

impl From<SequenceNumberCounter> for SharedSequenceNumberGenerator {
    fn from(counter: SequenceNumberCounter) -> Self {
        Arc::new(counter)
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    #[test]
    fn not_max_seqno() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(0x7FFF_FFFF_FFFF_FFFF);
        let _ = counter.next();
    }

    #[test]
    #[should_panic = "Ran out of sequence numbers"]
    fn max_seqno() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(0x8000_0000_0000_0000);
        let _ = counter.next();
    }
}
