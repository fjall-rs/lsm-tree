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
/// let tree = Config::new(path).open()?;
///
/// let seqno = SequenceNumberCounter::default();
///
/// // Do some inserts...
/// tree.insert("a".as_bytes(), "abc", seqno.next());
/// tree.insert("b".as_bytes(), "abc", seqno.next());
/// tree.insert("c".as_bytes(), "abc", seqno.next());
///
/// // Maybe create a snapshot
/// let snapshot = tree.snapshot(seqno.get());
///
/// // Create a batch
/// let batch_seqno = seqno.next();
/// tree.remove("a".as_bytes(), batch_seqno);
/// tree.remove("b".as_bytes(), batch_seqno);
/// tree.remove("c".as_bytes(), batch_seqno);
/// #
/// # assert!(tree.is_empty(None, None)?);
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

    /// Gets the next sequence number, without incrementing the counter.
    ///
    /// This should only be used when creating a snapshot.
    #[must_use]
    pub fn get(&self) -> SeqNo {
        self.0.load(Acquire)
    }

    /// Gets the next sequence number.
    #[must_use]
    pub fn next(&self) -> SeqNo {
        self.0.fetch_add(1, Release)
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
