// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::CompressionType;

/// Compression policy
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompressionPolicy(Vec<CompressionType>);

impl std::ops::Deref for CompressionPolicy {
    type Target = [CompressionType];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CompressionPolicy {
    pub(crate) fn get(&self, level: usize) -> CompressionType {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Disables all compression.
    #[must_use]
    pub fn disabled() -> Self {
        Self::all(CompressionType::None)
    }

    /// Uses the same compression in every level.
    #[must_use]
    pub fn all(c: CompressionType) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom compression policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    #[must_use]
    pub fn new(policy: impl Into<Vec<CompressionType>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy)
    }
}
