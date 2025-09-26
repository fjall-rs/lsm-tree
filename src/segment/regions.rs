// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{BlockHandle, BlockOffset};
use sfa::TocEntry;

fn toc_entry_to_handle(entry: &TocEntry) -> BlockHandle {
    BlockHandle::new(BlockOffset(entry.pos()), entry.len() as u32)
}

/// The regions block stores offsets to the different segment disk file "regions"
///
/// ----------------
/// |     data     | <- implicitly start at 0
/// |--------------|
/// |      tli     |
/// |--------------|
/// |     index    | <- may not exist (if full block index is used, TLI will be dense)
/// |--------------|
/// |    filter    | <- may not exist
/// |--------------|
/// |  ... TBD ... |
/// |--------------|
/// |     meta     |
/// |--------------|
/// |     toc      |
/// |--------------|
/// |   trailer    | <- fixed size
/// |--------------|
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedRegions {
    pub tli: BlockHandle,
    pub index: Option<BlockHandle>,
    pub filter: Option<BlockHandle>,
    pub metadata: BlockHandle,
}

impl ParsedRegions {
    pub fn parse_from_toc(toc: &sfa::Toc) -> crate::Result<Self> {
        Ok(Self {
            tli: toc
                .section(b"tli")
                .map(toc_entry_to_handle)
                .ok_or_else(|| {
                    log::error!("TLI should exist");
                    crate::Error::Unrecoverable
                })?,
            index: toc.section(b"index").map(toc_entry_to_handle),
            filter: toc.section(b"filter").map(toc_entry_to_handle),
            metadata: toc
                .section(b"meta")
                .map(toc_entry_to_handle)
                .ok_or_else(|| {
                    log::error!("Metadata should exist");
                    crate::Error::Unrecoverable
                })?,
        })
    }
}
