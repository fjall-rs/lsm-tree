// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::AtomicU64;

#[derive(Debug, Default)]
pub struct GcStats {
    pub(crate) stale_items: AtomicU64,
    pub(crate) stale_bytes: AtomicU64,
}

impl GcStats {
    pub fn set_stale_items(&self, x: u64) {
        self.stale_items
            .store(x, std::sync::atomic::Ordering::Release);
    }

    pub fn set_stale_bytes(&self, x: u64) {
        self.stale_bytes
            .store(x, std::sync::atomic::Ordering::Release);
    }

    /// Returns the number of dead items in the blob file.
    pub fn stale_items(&self) -> u64 {
        self.stale_items.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the amount of dead bytes in the blob file.
    pub fn stale_bytes(&self) -> u64 {
        self.stale_bytes.load(std::sync::atomic::Ordering::Acquire)
    }
}
