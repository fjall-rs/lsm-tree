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

// TODO: remove default
impl Default for CompressionPolicy {
    fn default() -> Self {
        #[cfg(feature = "lz4")]
        let c = Self::new(&[CompressionType::None, CompressionType::Lz4]);

        #[cfg(not(feature = "lz4"))]
        let c = Self::new(&[CompressionType::None]);

        c
    }
}

impl CompressionPolicy {
    pub(crate) fn get(&self, level: usize) -> CompressionType {
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    // TODO: accept Vec... Into<Vec<...>>? or owned

    /// Uses the same compression in every level.
    #[must_use]
    pub fn all(c: CompressionType) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom compression policy.
    ///
    /// # Example
    ///
    /// Skip compression in level 0:
    ///
    /// ```
    /// # use lsm_tree::{CompressionType, config::CompressionPolicy};
    /// let policy = CompressionPolicy::new(&[
    ///   CompressionType::None,
    ///   CompressionType::Lz4, // use LZ4 for L1+
    /// ]);
    /// ```
    #[must_use]
    pub fn new(policy: &[CompressionType]) -> Self {
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy.into())
    }
}
