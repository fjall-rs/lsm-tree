// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{SeqNo, UserKey, ValueType};
use equivalent::{Comparable, Equivalent};
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

impl Equivalent<InternalKeyRef<'_>> for InternalKey {
    fn equivalent(&self, other: &InternalKeyRef<'_>) -> bool {
        self.user_key == other.user_key && self.seqno == other.seqno
    }
}

impl Comparable<InternalKeyRef<'_>> for InternalKey {
    fn compare(&self, other: &InternalKeyRef<'_>) -> std::cmp::Ordering {
        (&*self.user_key, Reverse(self.seqno)).cmp(&(other.user_key, Reverse(other.seqno)))
    }
}

/// Temporary internal key without heap allocation
#[derive(Debug, Eq)]
pub struct InternalKeyRef<'a> {
    pub user_key: &'a [u8],
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl<'a> InternalKeyRef<'a> {
    pub fn new(user_key: &'a [u8], seqno: u64, value_type: ValueType) -> Self {
        InternalKeyRef {
            user_key,
            seqno,
            value_type,
        }
    }
}

impl<'a> PartialEq for InternalKeyRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.seqno == other.seqno
    }
}

impl<'a> PartialOrd for InternalKeyRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for InternalKeyRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}

impl Equivalent<InternalKey> for InternalKeyRef<'_> {
    fn equivalent(&self, other: &InternalKey) -> bool {
        self.user_key == other.user_key && self.seqno == other.seqno
    }
}

impl Comparable<InternalKey> for InternalKeyRef<'_> {
    fn compare(&self, other: &InternalKey) -> std::cmp::Ordering {
        (self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}
