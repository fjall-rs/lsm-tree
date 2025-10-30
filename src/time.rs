// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    #[cfg(test)]
    {
        if let Some(cell) = NOW_OVERRIDE.get() {
            if let Some(override_val) = *cell.lock().expect("lock is poisoned") {
                return override_val;
            }
        }
    }

    let now = std::time::SystemTime::now();

    #[expect(clippy::expect_used, reason = "trivial")]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time went backwards")
}

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
static NOW_OVERRIDE: OnceLock<Mutex<Option<std::time::Duration>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn set_unix_timestamp_for_test(value: Option<std::time::Duration>) {
    let cell = NOW_OVERRIDE.get_or_init(|| Mutex::new(None));
    *cell.lock().expect("lock is poisoned") = value;
}
