// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::GlobalTableId;
use crate::{descriptor_table::DescriptorTable, fs::FileSystem};
use std::sync::Arc;

/// Allows accessing a file (either cached or pinned)
pub enum FileAccessor<F: FileSystem> {
    /// Pinned file descriptor
    ///
    /// This is used in case file descriptor cache is `None` (to skip cache lookups)
    File(Arc<F::File>),

    /// Access to file descriptor cache
    DescriptorTable(Arc<DescriptorTable<F>>),
}

impl<F: FileSystem> FileAccessor<F> {
    pub fn as_descriptor_table(&self) -> Option<&DescriptorTable<F>> {
        match self {
            Self::DescriptorTable(d) => Some(d),
            Self::File(_) => None,
        }
    }

    #[must_use]
    pub fn access_for_table(&self, table_id: &GlobalTableId) -> Option<Arc<F::File>> {
        match self {
            Self::File(fd) => Some(fd.clone()),
            Self::DescriptorTable(descriptor_table) => descriptor_table.access_for_table(table_id),
        }
    }

    pub fn insert_for_table(&self, table_id: GlobalTableId, fd: Arc<F::File>) {
        if let Self::DescriptorTable(descriptor_table) = self {
            descriptor_table.insert_for_table(table_id, fd);
        }
    }

    #[must_use]
    pub fn access_for_blob_file(&self, table_id: &GlobalTableId) -> Option<Arc<F::File>> {
        match self {
            Self::File(fd) => Some(fd.clone()),
            Self::DescriptorTable(descriptor_table) => {
                descriptor_table.access_for_blob_file(table_id)
            }
        }
    }

    pub fn insert_for_blob_file(&self, table_id: GlobalTableId, fd: Arc<F::File>) {
        if let Self::DescriptorTable(descriptor_table) = self {
            descriptor_table.insert_for_blob_file(table_id, fd);
        }
    }
}

impl<F: FileSystem> Clone for FileAccessor<F> {
    fn clone(&self) -> Self {
        match self {
            Self::File(fd) => Self::File(fd.clone()),
            Self::DescriptorTable(descriptor_table) => {
                Self::DescriptorTable(descriptor_table.clone())
            }
        }
    }
}

impl<F: FileSystem> std::fmt::Debug for FileAccessor<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::File(_) => write!(f, "FileAccessor::Pinned"),
            Self::DescriptorTable(_) => {
                write!(f, "FileAccessor::Cached")
            }
        }
    }
}
