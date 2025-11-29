// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::SeqNo;
use std::sync::{
    atomic::{
        AtomicU64,
        Ordering::{AcqRel, Acquire, Release},
    },
    Arc,
};

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
///
/// let tree = Config::new(path, seqno.clone()).open()?;
///
/// // Do some inserts...
/// tree.insert("a".as_bytes(), "abc", seqno.next());
/// tree.insert("b".as_bytes(), "abc", seqno.next());
/// tree.insert("c".as_bytes(), "abc", seqno.next());
///
/// // Create a batch
/// let batch_seqno = seqno.next();
/// tree.remove("a".as_bytes(), batch_seqno);
/// tree.remove("b".as_bytes(), batch_seqno);
/// tree.remove("c".as_bytes(), batch_seqno);
/// #
/// # assert!(tree.is_empty(batch_seqno + 1, None)?);
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

    /// Gets the would-be-next sequence number, without incrementing the counter.
    ///
    /// This should only be used when creating a snapshot.
    #[must_use]
    pub fn get(&self) -> SeqNo {
        self.0.load(Acquire)
    }

    /// Gets the next sequence number.
    #[must_use]
    #[allow(clippy::missing_panics_doc, reason = "we should never run out of u64s")]
    pub fn next(&self) -> SeqNo {
        let seqno = self.0.fetch_add(1, Release);

        // The MSB is reserved for transactions.
        //
        // This gives us 63-bit sequence numbers technically.
        assert!(seqno < 0x8000_0000_0000_0000, "Ran out of sequence numbers");

        seqno
    }

    /// Sets the sequence number.
    pub fn set(&self, seqno: SeqNo) {
        self.0.store(seqno, Release);
    }

    /// Maximizes the sequence number.
    pub fn fetch_max(&self, seqno: SeqNo) {
        self.0.fetch_max(seqno, AcqRel);
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
