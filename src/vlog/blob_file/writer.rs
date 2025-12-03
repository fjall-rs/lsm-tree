// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::meta::Metadata;
use crate::{
    checksum::ChecksummedWriter, time::unix_timestamp, vlog::BlobFileId, Checksum, CompressionType,
    KeyRange, SeqNo, UserKey,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub const BLOB_HEADER_MAGIC: &[u8] = b"BLOB";

pub const BLOB_HEADER_LEN: usize = BLOB_HEADER_MAGIC.len()
    + std::mem::size_of::<u128>() // Checksum
    + std::mem::size_of::<u64>() // SeqNo
    + std::mem::size_of::<u16>() // Key length
    + std::mem::size_of::<u32>() // Real value length
    + std::mem::size_of::<u32>(); // On-disk value length

/// Blob file writer
pub struct Writer {
    pub path: PathBuf,
    pub(crate) blob_file_id: BlobFileId,

    #[expect(clippy::struct_field_names)]
    writer: sfa::Writer<ChecksummedWriter<BufWriter<File>>>,

    offset: u64,

    pub(crate) item_count: u64,
    pub(crate) written_blob_bytes: u64,
    pub(crate) uncompressed_bytes: u64,

    pub(crate) first_key: Option<UserKey>,
    pub(crate) last_key: Option<UserKey>,

    pub(crate) compression: CompressionType,
}

impl Writer {
    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(path: P, blob_file_id: BlobFileId) -> crate::Result<Self> {
        let path = path.as_ref();

        let writer = BufWriter::new(File::create(path)?);
        let writer = ChecksummedWriter::new(writer);
        let mut writer = sfa::Writer::from_writer(writer);
        writer.start("data")?;

        Ok(Self {
            path: path.into(),
            blob_file_id,

            writer,

            offset: 0,
            item_count: 0,
            written_blob_bytes: 0,
            uncompressed_bytes: 0,

            first_key: None,
            last_key: None,

            compression: CompressionType::None,
        })
    }

    pub fn use_compression(mut self, compressor: CompressionType) -> Self {
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

    pub(crate) fn write_raw(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        uncompressed_len: u32,
    ) -> crate::Result<u32> {
        assert!(!key.is_empty());
        assert!(u16::try_from(key.len()).is_ok());
        assert!(u32::try_from(value.len()).is_ok());

        if self.first_key.is_none() {
            self.first_key = Some(key.into());
        }
        self.last_key = Some(key.into());

        self.uncompressed_bytes += u64::from(uncompressed_len);

        // NOTE:
        // BLOB HEADER LAYOUT
        //
        // [MAGIC_BYTES; 4B]
        // [Checksum; 16B]
        // [Seqno; 8B]
        // [key len; 2B]
        // [real val len; 4B]
        // [on-disk val len; 4B]
        // [...key; ?]
        // [...val; ?]

        // Write header
        self.writer.write_all(BLOB_HEADER_MAGIC)?;

        let value = match &self.compression {
            CompressionType::None => std::borrow::Cow::Borrowed(value),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => std::borrow::Cow::Owned(lz4_flex::compress(value)),
        };

        let checksum = {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            hasher.update(key);
            hasher.update(&value);
            hasher.digest128()
        };

        // Write checksum
        self.writer.write_u128::<LittleEndian>(checksum)?;

        // Write seqno
        self.writer.write_u64::<LittleEndian>(seqno)?;

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        self.writer.write_u16::<LittleEndian>(key.len() as u16)?;

        // Write uncompressed value length
        self.writer.write_u32::<LittleEndian>(uncompressed_len)?;

        // Write compressed (on-disk) value length
        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        self.writer.write_u32::<LittleEndian>(value.len() as u32)?;

        self.writer.write_all(key)?;
        self.writer.write_all(&value)?;

        // Update offset
        self.offset += BLOB_HEADER_MAGIC.len() as u64;
        self.offset += std::mem::size_of::<u128>() as u64;
        self.offset += std::mem::size_of::<u64>() as u64;

        self.offset += std::mem::size_of::<u16>() as u64;
        self.offset += std::mem::size_of::<u32>() as u64;
        self.offset += std::mem::size_of::<u32>() as u64;

        self.offset += key.len() as u64;
        self.offset += value.len() as u64;

        // Update metadata
        self.written_blob_bytes += value.len() as u64;
        self.item_count += 1;

        // TODO: if we store the offset before writing, we can return a vhandle here
        // instead of needing to call offset() and blob_file_id() before write()

        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        Ok(value.len() as u32)
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
    pub fn write(&mut self, key: &[u8], seqno: SeqNo, value: &[u8]) -> crate::Result<u32> {
        self.write_raw(key, seqno, value, value.len() as u32)
    }

    pub(crate) fn finish(mut self) -> crate::Result<(Metadata, Checksum)> {
        self.writer.start("meta")?;

        // Write metadata
        let metadata = Metadata {
            id: self.blob_file_id,
            created_at: unix_timestamp().as_nanos(),
            item_count: self.item_count,
            total_compressed_bytes: self.written_blob_bytes,
            total_uncompressed_bytes: self.uncompressed_bytes,
            key_range: KeyRange::new((
                self.first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                self.last_key
                    .clone()
                    .expect("should have written at least 1 item"),
            )),
            compression: self.compression,
        };
        metadata.encode_into(&mut self.writer)?;

        let mut checksum = self.writer.into_inner()?;
        checksum.inner_mut().get_mut().sync_all()?;
        let checksum = checksum.checksum();

        Ok((metadata, checksum))
    }
}
