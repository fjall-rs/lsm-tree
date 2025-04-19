// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod bit_array;
mod blocked;
mod standard;

pub use blocked::BlockedBloomFilter;
pub use standard::StandardBloomFilter;

/// Two hashes that are used for double hashing
pub type CompositeHash = (u64, u64);
