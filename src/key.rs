// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{comparator::UserComparator, SeqNo, UserKey, ValueType};
use std::cmp::Reverse;

#[derive(Clone, Eq)]
pub struct InternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.seqno == other.seqno
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl std::fmt::Debug for InternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
                ValueType::WeakTombstone => "W",
                ValueType::MergeOperand => "M",
                ValueType::Indirection => "Vb",
            },
        )
    }
}

impl InternalKey {
    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, value_type: ValueType) -> Self {
        let user_key = user_key.into();

        assert!(
            u16::try_from(user_key.len()).is_ok(),
            "keys can be 65535 bytes in length",
        );

        Self {
            user_key,
            seqno,
            value_type,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_type.is_tombstone()
    }

    /// Compares two internal keys using a custom user key comparator.
    ///
    /// User keys are compared via the given comparator; ties are broken
    /// by sequence number in descending order (higher seqno = "smaller"
    /// in sort order), matching the invariant of [`Ord for InternalKey`].
    pub(crate) fn compare_with(
        &self,
        other: &Self,
        cmp: &dyn UserComparator,
    ) -> std::cmp::Ordering {
        cmp.compare(&self.user_key, &other.user_key)
            .then_with(|| Reverse(self.seqno).cmp(&Reverse(other.seqno)))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}
