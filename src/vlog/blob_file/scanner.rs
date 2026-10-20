// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    value::{UserKey, UserValue},
    vlog::BlobFileId,
    BlobFile, SeqNo,
};

#[derive(Debug, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: UserKey,
    pub seqno: SeqNo,
    pub value: UserValue,
    pub offset: u64,
    pub uncompressed_len: u32,
}

trait AbstractBlobFileScanner: Iterator<Item = crate::Result<ScanEntry>> {
    fn blob_file_id(&self) -> BlobFileId;
}

impl AbstractBlobFileScanner for crate::vlog::blob_file::scanner_v3::Scanner {
    fn blob_file_id(&self) -> BlobFileId {
        self.blob_file_id
    }
}

impl AbstractBlobFileScanner for crate::vlog::blob_file::scanner_v4::Scanner {
    fn blob_file_id(&self) -> BlobFileId {
        self.blob_file_id
    }
}

pub struct BlobFileScanner(Box<dyn AbstractBlobFileScanner>);

impl Iterator for BlobFileScanner {
    type Item = crate::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl BlobFileScanner {
    pub fn new(blob_file: &BlobFile) -> crate::Result<Self> {
        Ok(Self(if blob_file.is_v3() {
            Box::new(super::scanner_v3::Scanner::new(
                &blob_file.0.path,
                blob_file.id(),
            )?)
        } else {
            Box::new(super::scanner_v4::Scanner::new(
                &blob_file.0.path,
                blob_file.id(),
                blob_file.len() as usize,
            )?)
        }))
    }

    pub fn blob_file_id(&self) -> BlobFileId {
        self.0.blob_file_id()
    }
}
