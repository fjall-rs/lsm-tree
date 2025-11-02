// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Hash ratio policy
#[derive(Debug, Clone, PartialEq)]
pub struct HashRatioPolicy(Vec<f32>);

impl std::ops::Deref for HashRatioPolicy {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl HashRatioPolicy {
    pub(crate) fn get(&self, level: usize) -> f32 {
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Uses the same block size in every level.
    #[must_use]
    pub fn all(c: f32) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom block size policy.
    #[must_use]
    pub fn new(policy: impl Into<Vec<f32>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy)
    }
}
