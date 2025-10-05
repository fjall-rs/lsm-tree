// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::Writer;
use crate::{
    vlog::{BlobFileId, ValueHandle},
    CompressionType, SequenceNumberCounter,
};
use std::path::{Path, PathBuf};

/// Blob file writer, may write multiple blob files
pub struct MultiWriter {
    folder: PathBuf,
    target_size: u64,

    writers: Vec<Writer>,

    id_generator: SequenceNumberCounter,

    compression: CompressionType,
}

impl MultiWriter {
    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(
        id_generator: SequenceNumberCounter,
        target_size: u64,
        folder: P,
    ) -> std::io::Result<Self> {
        let folder = folder.as_ref();

        let blob_file_id = id_generator.next();
        let blob_file_path = folder.join(blob_file_id.to_string());

        Ok(Self {
            id_generator,
            folder: folder.into(),
            target_size,

            writers: vec![Writer::new(blob_file_path, blob_file_id)?],

            compression: CompressionType::None,
        })
    }

    /// Sets the blob file target size.
    #[must_use]
    pub fn use_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }

    /// Sets the compression method.
    #[must_use]
    #[doc(hidden)]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression.clone_from(&compression);
        self.get_active_writer_mut().compression = compression;
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_active_writer(&self) -> &Writer {
        // NOTE: initialized in constructor
        #[allow(clippy::expect_used)]
        self.writers.last().expect("should exist")
    }

    fn get_active_writer_mut(&mut self) -> &mut Writer {
        // NOTE: initialized in constructor
        #[allow(clippy::expect_used)]
        self.writers.last_mut().expect("should exist")
    }

    #[must_use]
    pub fn offset(&self) -> u64 {
        self.get_active_writer().offset()
    }

    #[must_use]
    pub fn blob_file_id(&self) -> BlobFileId {
        self.get_active_writer().blob_file_id()
    }

    /// Sets up a new writer for the next blob file.
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating blob file writer");

        let new_blob_file_id = self.id_generator.next();
        let blob_file_path = self.folder.join(new_blob_file_id.to_string());

        let new_writer =
            Writer::new(blob_file_path, new_blob_file_id)?.use_compression(self.compression);

        self.writers.push(new_writer);

        Ok(())
    }

    /// Writes an item.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> crate::Result<u32> {
        let key = key.as_ref();
        let value = value.as_ref();

        let target_size = self.target_size;

        // Write actual value into blob file
        let writer = self.get_active_writer_mut();
        let bytes_written = writer.write(key, value)?;

        // Check for blob file size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            writer.flush()?;
            self.rotate()?;
        }

        Ok(bytes_written)
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<Writer>> {
        let writer = self.get_active_writer_mut();

        if writer.item_count > 0 {
            writer.flush()?;
        }

        Ok(self.writers)
    }
}
