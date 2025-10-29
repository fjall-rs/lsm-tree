// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    #[expect(clippy::expect_used, reason = "trivial")]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time went backwards")
}
