// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{file_offsets::FileOffsets, meta::Metadata};
use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, Write},
    path::Path,
};

pub const TRAILER_SIZE: usize = 256;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct SegmentFileTrailer {
    #[doc(hidden)]
    pub metadata: Metadata,

    #[doc(hidden)]
    pub offsets: FileOffsets,
}

impl SegmentFileTrailer {
    pub fn from_file(path: &Path) -> crate::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::End(-(TRAILER_SIZE as i64)))?;

        // Parse pointers
        let offsets = FileOffsets::decode_from(&mut reader)?;

        let remaining_padding = TRAILER_SIZE - FileOffsets::serialized_len() - MAGIC_BYTES.len();
        reader.seek_relative(remaining_padding as i64)?;

        // Check trailer magic
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(crate::Error::Decode(DecodeError::InvalidHeader(
                "SegmentTrailer",
            )));
        }

        log::trace!("Trailer offsets: {offsets:#?}");

        // Jump to metadata and parse
        reader.seek(std::io::SeekFrom::Start(*offsets.metadata_ptr))?;
        let metadata = Metadata::decode_from(&mut reader)?;

        Ok(Self { metadata, offsets })
    }
}

impl Encode for SegmentFileTrailer {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        let mut v = Vec::with_capacity(TRAILER_SIZE);

        // TODO: 3.0.0, magic header, too?

        self.offsets.encode_into(&mut v)?;

        // Pad with remaining bytes
        v.resize(TRAILER_SIZE - MAGIC_BYTES.len(), 0);

        v.write_all(&MAGIC_BYTES)?;

        assert_eq!(
            v.len(),
            TRAILER_SIZE,
            "segment file trailer has invalid size"
        );

        writer.write_all(&v)?;

        Ok(())
    }
}
