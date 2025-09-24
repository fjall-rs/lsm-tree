// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub use crate::segment::filter::BloomConstructionPolicy;

/// Filter policy entry
///
/// Each level can be configured with a different filter type and bits per key
#[derive(Copy, Debug, Clone, PartialEq)]
pub enum FilterPolicyEntry {
    /// Skip filter construction
    None,

    /// Standard bloom filter with K bits per key
    Bloom(BloomConstructionPolicy),
}

/// Filter policy
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPolicy(Vec<FilterPolicyEntry>);

impl std::ops::Deref for FilterPolicy {
    type Target = [FilterPolicyEntry];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// TODO: remove default
impl Default for FilterPolicy {
    fn default() -> Self {
        Self::new(&[FilterPolicyEntry::Bloom(
            BloomConstructionPolicy::BitsPerKey(10.0),
        )])
    }
}

impl FilterPolicy {
    pub(crate) fn get(&self, level: usize) -> FilterPolicyEntry {
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    // TODO: accept Vec... Into<Vec<...>>? or owned

    /// Uses the same block size in every level.
    #[must_use]
    pub fn all(c: FilterPolicyEntry) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom block size policy.
    #[must_use]
    pub fn new(policy: &[FilterPolicyEntry]) -> Self {
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy.into())
    }
}
