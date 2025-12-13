// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Restart interval policy
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RestartIntervalPolicy(Vec<u8>);

impl std::ops::Deref for RestartIntervalPolicy {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RestartIntervalPolicy {
    pub(crate) fn get(&self, level: usize) -> u8 {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    // TODO: accept Vec... Into<Vec<...>>? or owned

    /// Uses the same block size in every level.
    #[must_use]
    pub fn all(c: u8) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom block size policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    #[must_use]
    pub fn new(policy: impl Into<Vec<u8>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy)
    }
}
