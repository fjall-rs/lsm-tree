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
/// # Invariants
///
/// Implementors **must** respect the following invariants, which are
/// relied upon by the storage engine (e.g., for MVCC and transactions):
///
/// - The most-significant bit (MSB) of a sequence number is reserved
///   for internal use by the transaction layer.
/// - All sequence numbers produced by this generator **must** therefore
///   be strictly less than `0x8000_0000_0000_0000`.
/// - Calls to [`SequenceNumberGenerator::next`] on a given generator
///   instance must return monotonically increasing values (each `next`
///   returns a value that is strictly greater than any value previously
///   returned by `next` on the same instance) until the reserved MSB
///   boundary is reached.
/// - In practice, this means `next`-generated sequence numbers are
///   unique for the lifetime of the generator (modulo wrap-around, which
///   must not occur within the reserved MSB range).
///
/// Implementors are also responsible for ensuring that [`get`],
/// [`set`], and [`fetch_max`] do not violate these invariants (e.g., by
/// setting the counter to a value at or above `0x8000_0000_0000_0000`,
/// or by moving the counter backwards such that subsequent calls to
/// [`next`] would observe non-monotonic sequence numbers).
///
/// # Supertraits
///
/// `UnwindSafe + RefUnwindSafe` are required because generators may be
/// captured inside `catch_unwind` (e.g., during ingestion error recovery).
///
/// The default implementation is [`SequenceNumberCounter`], which uses
/// an `Arc<AtomicU64>` for lock-free atomic operations. It enforces the
/// MSB boundary in `new`, `set`, and `fetch_max` (via assert/clamp),
/// and panics in `next` if the counter reaches the reserved range.
pub trait SequenceNumberGenerator:
    Send + Sync + Debug + std::panic::UnwindSafe + std::panic::RefUnwindSafe + 'static
{
    /// Gets the next sequence number, atomically incrementing the counter.
    ///
    /// This must:
    /// - Return a value strictly greater than any value previously returned
    ///   by `next` on the same generator instance (monotonic increase).
    /// - Never return a value greater than or equal to
    ///   `0x8000_0000_0000_0000`, since the MSB is reserved.
    #[must_use]
    fn next(&self) -> SeqNo;

    /// Gets the current sequence number without incrementing.
    ///
    /// This should be consistent with the value that will be used as the
    /// basis for the next call to [`next`], and must not itself alter the
    /// monotonicity guarantees.
    #[must_use]
    fn get(&self) -> SeqNo;

    /// Sets the sequence number to the given value.
    ///
    /// Implementations must ensure:
    /// - `value` is strictly less than `0x8000_0000_0000_0000`.
    /// - Subsequent calls to [`next`] continue to produce monotonically
    ///   increasing values, i.e., `next` must not observe a value lower
    ///   than one it has already returned.
    fn set(&self, value: SeqNo);

    /// Atomically updates the sequence number to the maximum of
    /// the current value and the given value.
    ///
    /// The resulting stored value must:
    /// - Remain strictly less than `0x8000_0000_0000_0000`.
    /// - Preserve the monotonicity guarantees expected by [`next`].
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
    /// Creates a new counter, setting it to some previous value.
    ///
    /// # Panics
    ///
    /// Panics if `prev >= 0x8000_0000_0000_0000` (reserved MSB range).
    #[must_use]
    pub fn new(prev: SeqNo) -> Self {
        assert!(
            prev < 0x8000_0000_0000_0000,
            "Sequence number must not use the reserved MSB"
        );
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
        // NOTE: We use fetch_add (not a CAS loop) for performance. The internal
        // counter may briefly hold a reserved-range value between the fetch_add
        // and the assert, but this is acceptable because (a) the assert panics
        // immediately, and (b) set/fetch_max enforce the boundary on input.
        let seqno = self.0.fetch_add(1, AcqRel);

        // The MSB is reserved for transactions, limiting us to 63-bit seqnos.
        assert!(seqno < 0x8000_0000_0000_0000, "Ran out of sequence numbers");

        seqno
    }

    fn get(&self) -> SeqNo {
        self.0.load(Acquire)
    }

    fn set(&self, value: SeqNo) {
        assert!(
            value < 0x8000_0000_0000_0000,
            "Sequence number must not use the reserved MSB"
        );
        self.0.store(value, Release);
    }

    fn fetch_max(&self, value: SeqNo) {
        // Clamp to the maximum allowed sequence number to avoid storing
        // a value in the reserved MSB range.
        let clamped = value.min(0x7FFF_FFFF_FFFF_FFFF);
        self.0.fetch_max(clamped, AcqRel);
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
        // One below the boundary: next() returns 0x7FFF_FFFF_FFFF_FFFE,
        // which is valid (< 0x8000_0000_0000_0000).
        counter.set(0x7FFF_FFFF_FFFF_FFFE);
        let _ = counter.next();
    }

    #[test]
    #[should_panic = "Ran out of sequence numbers"]
    fn max_seqno() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(0x7FFF_FFFF_FFFF_FFFF);
        // First call returns 0x7FFF_FFFF_FFFF_FFFF (valid), counter
        // increments to 0x8000_0000_0000_0000 internally.
        let _ = counter.next();
        // Second call returns 0x8000_0000_0000_0000 and panics.
        let _ = counter.next();
    }

    #[test]
    #[should_panic = "Sequence number must not use the reserved MSB"]
    fn set_reserved_range() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(0x8000_0000_0000_0000);
    }

    #[test]
    fn fetch_max_clamps_reserved() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(100);
        counter.fetch_max(0x8000_0000_0000_0000);
        // Should be clamped to MAX allowed value
        assert_eq!(counter.get(), 0x7FFF_FFFF_FFFF_FFFF);
    }
}
