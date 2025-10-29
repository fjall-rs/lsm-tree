// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::path::{Path, PathBuf};

#[allow(clippy::module_name_repetitions)]
pub fn absolute_path(path: &Path) -> PathBuf {
    // Not sure if this can even fail realistically
    #[expect(clippy::expect_used, reason = "not much we can do about it")]
    std::path::absolute(path).expect("should be absolute path")
}
