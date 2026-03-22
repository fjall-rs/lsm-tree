// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::UserValue;
use std::panic::RefUnwindSafe;

/// A user-defined merge operator for commutative LSM operations.
///
/// Merge operators enable efficient read-modify-write operations by storing
/// partial updates (operands) that are lazily combined during reads and
/// compaction, avoiding the need for explicit read-modify-write cycles.
///
/// # Implementor contract
///
/// The merge function must be **deterministic and stable across multiple
/// passes**. The `base_value` may itself be the result of a previous merge
/// (e.g., from compaction or an earlier read resolution) rather than the
/// original stored value. Repeated merging must produce identical bytes
/// for the same logical state.
///
/// # Examples
///
/// A simple counter merge operator that sums integer operands:
///
/// ```
/// use lsm_tree::{MergeOperator, UserValue};
///
/// struct CounterMerge;
///
/// impl MergeOperator for CounterMerge {
///     fn merge(
///         &self,
///         _key: &[u8],
///         base_value: Option<&[u8]>,
///         operands: &[&[u8]],
///     ) -> lsm_tree::Result<UserValue> {
///         let mut counter: i64 = match base_value {
///             Some(bytes) if bytes.len() == 8 => i64::from_le_bytes(
///                 bytes.try_into().expect("checked length"),
///             ),
///             Some(_) => return Err(lsm_tree::Error::MergeOperator),
///             None => 0,
///         };
///
///         for operand in operands {
///             if operand.len() != 8 {
///                 return Err(lsm_tree::Error::MergeOperator);
///             }
///             counter += i64::from_le_bytes(
///                 (*operand).try_into().expect("checked length"),
///             );
///         }
///
///         Ok(counter.to_le_bytes().to_vec().into())
///     }
/// }
/// ```
pub trait MergeOperator: Send + Sync + RefUnwindSafe + 'static {
    /// Merges operands with an optional base value.
    ///
    /// `key` is the user key being merged.
    ///
    /// `base_value` is the existing value for the key, or `None` if no base
    /// value exists (e.g., the key was never written or was deleted). This
    /// may already be the output of a previous `merge` call (after compaction
    /// or an earlier read), so implementations must be stable when re-merging.
    ///
    /// `operands` contains the merge operand values in ascending sequence
    /// number order (chronological — oldest first).
    ///
    /// Returns the merged value on success.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::MergeOperator`] if the merge fails (e.g., corrupted
    /// operand data).
    fn merge(
        &self,
        key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> crate::Result<UserValue>;
}
