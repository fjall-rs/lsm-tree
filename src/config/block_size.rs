// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Block size policy
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlockSizePolicy(Vec<u32>);

impl std::ops::Deref for BlockSizePolicy {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BlockSizePolicy {
    pub(crate) fn get(&self, level: usize) -> u32 {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Uses the same block size in every level.
    #[must_use]
    pub fn all(c: u32) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom block size policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    #[must_use]
    pub fn new(policy: impl Into<Vec<u32>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy)
    }
}
