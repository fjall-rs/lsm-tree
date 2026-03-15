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

/// The maximum allowed sequence number value.
///
/// The MSB (`0x8000_0000_0000_0000`) is reserved for internal use by the
/// transaction layer, so all sequence numbers must be strictly less than
/// this boundary. Values returned by [`SequenceNumberGenerator::next`]
/// must be at most `MAX_SEQNO - 1` (`0x7FFF_FFFF_FFFF_FFFE`), leaving
/// room for `seqno + 1` operations used internally by the engine.
pub const MAX_SEQNO: SeqNo = 0x7FFF_FFFF_FFFF_FFFF;

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
/// Implementors are also responsible for ensuring that [`get`] does not
/// return values that violate these invariants, and that [`set`] and
/// [`fetch_max`] do not violate them (e.g., by setting the counter to a
/// value at or above `0x8000_0000_0000_0000`, or by moving the counter
/// backwards such that subsequent calls to [`next`] would observe
/// non-monotonic sequence numbers).
///
/// The [`set`] method is a special-case escape hatch intended for use
/// during initialization or recovery. It may move the counter forwards
/// or backwards and therefore **may** cause subsequent calls to
/// [`next`] to observe non-monotonic sequence numbers. This is
/// permitted; callers are responsible for using [`fetch_max`] (or other
/// application-level logic) after `set` if they need to reestablish any
/// monotonicity guarantees.
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
    /// - Never return a value greater than `MAX_SEQNO - 1`, leaving room
    ///   for internal `seqno + 1` operations that must remain strictly
    ///   less than [`MAX_SEQNO`].
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
    /// Implementations must ensure that `value` is strictly less than
    /// `0x8000_0000_0000_0000` (the MSB is reserved).
    ///
    /// This method is intended for direct overrides (e.g., during
    /// initialization or recovery) and is **not** required to preserve
    /// monotonicity with respect to values previously returned by
    /// [`next`]. In particular, calling `set` with a value lower than a
    /// previously returned `next` value may cause subsequent calls to
    /// [`next`] to observe non-monotonic sequence numbers. This is
    /// allowed; callers that need to advance the sequence number in a
    /// monotonic fashion should prefer [`fetch_max`] instead of `set`.
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
    /// Panics if `prev > MAX_SEQNO` (reserved MSB range).
    ///
    /// Note: `prev == MAX_SEQNO` is allowed but means the counter is
    /// exhausted — any subsequent call to [`next()`](Self::next) will panic.
    #[must_use]
    pub fn new(prev: SeqNo) -> Self {
        assert!(
            prev <= MAX_SEQNO,
            "Sequence number must not use the reserved MSB"
        );
        Self(Arc::new(AtomicU64::new(prev)))
    }

    /// Allocates and returns the next sequence number, atomically
    /// incrementing the internal counter.
    ///
    /// The returned value is the counter's value **before** the increment
    /// (i.e., pre-increment / fetch-then-add semantics).
    ///
    /// # Panics
    ///
    /// Panics if the current value is already [`MAX_SEQNO`] (i.e., when
    /// advancing past it would enter the reserved MSB range).
    #[must_use]
    pub fn next(&self) -> SeqNo {
        <Self as SequenceNumberGenerator>::next(self)
    }

    /// Returns the current internal counter value without incrementing.
    ///
    /// This is the value that the next call to [`next()`](Self::next) will
    /// return (and then advance past).
    #[must_use]
    pub fn get(&self) -> SeqNo {
        <Self as SequenceNumberGenerator>::get(self)
    }

    /// Sets the sequence number to the given value.
    ///
    /// # Panics
    ///
    /// Panics if `value > MAX_SEQNO` (reserved MSB range).
    pub fn set(&self, value: SeqNo) {
        <Self as SequenceNumberGenerator>::set(self, value);
    }

    /// Atomically updates the sequence number to the maximum of
    /// the current value and the provided value.
    pub fn fetch_max(&self, value: SeqNo) {
        <Self as SequenceNumberGenerator>::fetch_max(self, value);
    }
}

impl SequenceNumberGenerator for SequenceNumberCounter {
    fn next(&self) -> SeqNo {
        // We use fetch_update (CAS loop) instead of fetch_add to ensure the
        // internal counter never enters the reserved MSB range. With fetch_add,
        // a caught panic (via catch_unwind, which the trait requires via
        // UnwindSafe) would leave the counter permanently stuck at an invalid
        // value. fetch_update only stores the new value on success.
        match self.0.fetch_update(AcqRel, Acquire, |current| {
            if current >= MAX_SEQNO {
                None
            } else {
                Some(current + 1)
            }
        }) {
            Ok(seqno) => seqno,
            Err(current) => panic!("Ran out of sequence numbers (current: {current})"),
        }
    }

    fn get(&self) -> SeqNo {
        self.0.load(Acquire)
    }

    fn set(&self, value: SeqNo) {
        assert!(
            value <= MAX_SEQNO,
            "Sequence number must not use the reserved MSB"
        );
        self.0.store(value, Release);
    }

    fn fetch_max(&self, value: SeqNo) {
        // Clamp to the maximum allowed sequence number to avoid storing
        // a value in the reserved MSB range.
        let clamped = value.min(MAX_SEQNO);
        self.0.fetch_max(clamped, AcqRel);
    }
}

// Orphan rules satisfied: SequenceNumberGenerator is a local trait,
// so Arc<dyn SequenceNumberGenerator> is covered by a local type parameter.
impl From<SequenceNumberCounter> for SharedSequenceNumberGenerator {
    fn from(counter: SequenceNumberCounter) -> Self {
        Arc::new(counter)
    }
}

#[cfg(test)]
mod tests {
    use super::MAX_SEQNO;
    use test_log::test;

    #[test]
    fn next_below_max_returns_valid_seqno() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(MAX_SEQNO - 1);
        let _ = counter.next();
    }

    #[test]
    #[should_panic(expected = "Ran out of sequence numbers")]
    fn next_at_max_panics() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(MAX_SEQNO);
        let _ = counter.next();
    }

    #[test]
    #[should_panic(expected = "Sequence number must not use the reserved MSB")]
    fn set_reserved_range_panics() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(MAX_SEQNO + 1);
    }

    #[test]
    fn fetch_max_clamps_reserved_to_max() {
        let counter = super::SequenceNumberCounter::default();
        counter.set(100);
        counter.fetch_max(MAX_SEQNO + 1);
        assert_eq!(counter.get(), MAX_SEQNO);
    }
}
