// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Fallback for platforms with no direct-I/O primitive (e.g. WASI).
//!
//! All `open_*_direct` functions return a regular buffered file handle. The direct-I/O
//! config knobs become silent no-ops. The `Config::open` callers do not need
//! `cfg` guards.

use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

pub fn open_read_direct(path: &Path) -> io::Result<File> {
    File::open(path)
}

pub fn create_write_direct(path: &Path) -> io::Result<File> {
    File::create_new(path)
}

pub fn create_or_truncate_write_direct(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}
