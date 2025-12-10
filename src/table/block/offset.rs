// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

// TODO: rename FileOffset?
#[derive(Copy, Clone, Default, Debug, std::hash::Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct BlockOffset(pub u64);

impl std::ops::Deref for BlockOffset {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::AddAssign<Self> for BlockOffset {
    fn add_assign(&mut self, rhs: Self) {
        *self += *rhs;
    }
}

impl std::ops::AddAssign<u64> for BlockOffset {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}
