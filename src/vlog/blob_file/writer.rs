// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{meta::Metadata, trailer::Trailer};
use crate::{
    coding::Encode,
    vlog::{compression::Compressor, BlobFileId},
    KeyRange, UserKey,
};
use byteorder::{BigEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

pub const BLOB_HEADER_MAGIC: &[u8] = &[b'V', b'L', b'G', b'B', b'L', b'O', b'B', 1];

/// Blob file writer
pub struct Writer<C: Compressor + Clone> {
    pub path: PathBuf,
    pub(crate) blob_file_id: BlobFileId,

    #[allow(clippy::struct_field_names)]
    active_writer: BufWriter<File>,

    offset: u64,

    pub(crate) item_count: u64,
    pub(crate) written_blob_bytes: u64,
    pub(crate) uncompressed_bytes: u64,

    pub(crate) first_key: Option<UserKey>,
    pub(crate) last_key: Option<UserKey>,

    pub(crate) compression: Option<C>,
}

impl<C: Compressor + Clone> Writer<C> {
    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(path: P, blob_file_id: BlobFileId) -> std::io::Result<Self> {
        let path = path.as_ref();

        let file = File::create(path)?;

        Ok(Self {
            path: path.into(),
            blob_file_id,

            active_writer: BufWriter::new(file),

            offset: 0,
            item_count: 0,
            written_blob_bytes: 0,
            uncompressed_bytes: 0,

            first_key: None,
            last_key: None,

            compression: None,
        })
    }

    pub fn use_compression(mut self, compressor: Option<C>) -> Self {
        self.compression = compressor;
        self
    }

    /// Returns the current offset in the file.
    ///
    /// This can be used to index an item into an external `Index`.
    #[must_use]
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the blob file ID.
    #[must_use]
    pub(crate) fn blob_file_id(&self) -> BlobFileId {
        self.blob_file_id
    }

    /// Writes an item into the file.
    ///
    /// Items need to be written in key order.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^32.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> crate::Result<u32> {
        assert!(!key.is_empty());
        assert!(u16::try_from(key.len()).is_ok());
        assert!(u32::try_from(value.len()).is_ok());

        if self.first_key.is_none() {
            self.first_key = Some(key.into());
        }
        self.last_key = Some(key.into());

        self.uncompressed_bytes += value.len() as u64;

        let value = match &self.compression {
            Some(compressor) => compressor.compress(value)?,
            None => value.to_vec(),
        };

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(key);
        hasher.update(&value);
        let checksum = hasher.digest();

        // TODO: 2.0.0 formalize blob header
        // into struct... store uncompressed len as well
        // so we can optimize rollover by avoiding
        // repeated compression & decompression

        // Write header
        self.active_writer.write_all(BLOB_HEADER_MAGIC)?;

        // Write checksum
        self.active_writer.write_u64::<BigEndian>(checksum)?;

        // Write key

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        self.active_writer
            .write_u16::<BigEndian>(key.len() as u16)?;
        self.active_writer.write_all(key)?;

        // Write value

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        self.active_writer
            .write_u32::<BigEndian>(value.len() as u32)?;
        self.active_writer.write_all(&value)?;

        // Header
        self.offset += BLOB_HEADER_MAGIC.len() as u64;

        // Checksum
        self.offset += std::mem::size_of::<u64>() as u64;

        // Key
        self.offset += std::mem::size_of::<u16>() as u64;
        self.offset += key.len() as u64;

        // Value
        self.offset += std::mem::size_of::<u32>() as u64;
        self.offset += value.len() as u64;

        // Update metadata
        self.written_blob_bytes += value.len() as u64;
        self.item_count += 1;

        // NOTE: Truncation is okay
        #[allow(clippy::cast_possible_truncation)]
        Ok(value.len() as u32)
    }

    pub(crate) fn flush(&mut self) -> crate::Result<()> {
        let metadata_ptr = self.active_writer.stream_position()?;

        // Write metadata
        let metadata = Metadata {
            item_count: self.item_count,
            compressed_bytes: self.written_blob_bytes,
            total_uncompressed_bytes: self.uncompressed_bytes,
            key_range: KeyRange::new((
                self.first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                self.last_key
                    .clone()
                    .expect("should have written at least 1 item"),
            )),
        };
        metadata.encode_into(&mut self.active_writer)?;

        Trailer {
            metadata,
            metadata_ptr,
        }
        .encode_into(&mut self.active_writer)?;

        self.active_writer.flush()?;
        self.active_writer.get_mut().sync_all()?;

        Ok(())
    }
}
