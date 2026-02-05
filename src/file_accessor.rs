// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::descriptor_table::DescriptorTable;
use crate::GlobalTableId;
use std::{fs::File, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Allows accessing a file (either cached or pinned)
#[derive(Clone)]
pub enum FileAccessor {
    File(Arc<File>),
    DescriptorTable(Arc<DescriptorTable>),
}

impl FileAccessor {
    pub fn as_descriptor_table(&self) -> Option<&DescriptorTable> {
        match self {
            Self::DescriptorTable(d) => Some(&d),
            Self::File(_) => None,
        }
    }

    #[must_use]
    pub fn access_for_table(
        &self,
        table_id: &GlobalTableId,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Option<Arc<File>> {
        match self {
            Self::File(fd) => Some(fd.clone()),
            Self::DescriptorTable(descriptor_table) => {
                #[cfg(feature = "metrics")]
                metrics
                    .table_file_opened_cached
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                descriptor_table.access_for_table(table_id)
            }
        }
    }

    pub fn insert_for_table(&self, table_id: GlobalTableId, fd: Arc<File>) {
        if let Self::DescriptorTable(descriptor_table) = self {
            descriptor_table.insert_for_table(table_id, fd);
        }
    }

    #[must_use]
    pub fn access_for_blob_file(&self, table_id: &GlobalTableId) -> Option<Arc<File>> {
        match self {
            Self::File(fd) => Some(fd.clone()),
            Self::DescriptorTable(descriptor_table) => {
                descriptor_table.access_for_blob_file(table_id)
            }
        }
    }

    pub fn insert_for_blob_file(&self, table_id: GlobalTableId, fd: Arc<File>) {
        if let Self::DescriptorTable(descriptor_table) = self {
            descriptor_table.insert_for_blob_file(table_id, fd);
        }
    }
}

impl std::fmt::Debug for FileAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::File(_) => write!(f, "FileAccessor::File(...)"),
            Self::DescriptorTable(_) => {
                write!(f, "FileAccessor::DescriptorTable(...)")
            }
        }
    }
}
