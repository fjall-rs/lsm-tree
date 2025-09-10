// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::Writer;
use crate::vlog::{compression::Compressor, value_log::IdGenerator, BlobFileId, ValueHandle};
use std::path::{Path, PathBuf};

/// Blob file writer, may write multiple blob files
pub struct MultiWriter<C: Compressor + Clone> {
    folder: PathBuf,
    target_size: u64,

    writers: Vec<Writer<C>>,

    id_generator: IdGenerator,

    compression: Option<C>,
}

impl<C: Compressor + Clone> MultiWriter<C> {
    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(
        id_generator: IdGenerator,
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

            compression: None,
        })
    }

    /// Sets the compression method
    #[must_use]
    #[doc(hidden)]
    pub fn use_compression(mut self, compressor: Option<C>) -> Self {
        self.compression.clone_from(&compressor);
        self.get_active_writer_mut().compression = compressor;
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_active_writer(&self) -> &Writer<C> {
        // NOTE: initialized in constructor
        #[allow(clippy::expect_used)]
        self.writers.last().expect("should exist")
    }

    fn get_active_writer_mut(&mut self) -> &mut Writer<C> {
        // NOTE: initialized in constructor
        #[allow(clippy::expect_used)]
        self.writers.last_mut().expect("should exist")
    }

    /// Returns the [`ValueHandle`] for the next written blob.
    ///
    /// This can be used to index an item into an external `Index`.
    #[must_use]
    pub fn get_next_value_handle(&self) -> ValueHandle {
        ValueHandle {
            offset: self.offset(),
            blob_file_id: self.blob_file_id(),
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.get_active_writer().offset()
    }

    #[must_use]
    fn blob_file_id(&self) -> BlobFileId {
        self.get_active_writer().blob_file_id()
    }

    /// Sets up a new writer for the next blob file.
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating blob file writer");

        let new_blob_file_id = self.id_generator.next();
        let blob_file_path = self.folder.join(new_blob_file_id.to_string());

        let new_writer = Writer::new(blob_file_path, new_blob_file_id)?
            .use_compression(self.compression.clone());

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

    pub(crate) fn finish(mut self) -> crate::Result<Vec<Writer<C>>> {
        let writer = self.get_active_writer_mut();

        if writer.item_count > 0 {
            writer.flush()?;
        }

        Ok(self.writers)
    }
}
