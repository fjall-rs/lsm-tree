// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::Writer;
use crate::fs::FsFile;
use crate::{
    file_accessor::FileAccessor,
    vlog::{
        blob_file::{Inner as BlobFileInner, Metadata},
        ValueHandle,
    },
    BlobFile, CompressionType, DescriptorTable, SeqNo, SequenceNumberCounter, TreeId,
};
use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

/// Blob file writer, may write multiple blob files
pub struct MultiWriter {
    folder: PathBuf,
    target_size: u64,

    active_writer: Writer,

    results: Vec<BlobFile>,

    id_generator: SequenceNumberCounter,

    compression: CompressionType,
    passthrough_compression: CompressionType,

    tree_id: TreeId,
    descriptor_table: Option<Arc<DescriptorTable>>,
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
        folder: P,
        tree_id: TreeId,
        descriptor_table: Option<Arc<DescriptorTable>>,
    ) -> crate::Result<Self> {
        let folder = folder.as_ref();

        let blob_file_id = id_generator.next();
        let blob_file_path = folder.join(blob_file_id.to_string());

        Ok(Self {
            id_generator,
            folder: folder.into(),
            target_size: 64 * 1_024 * 1_024,

            active_writer: Writer::new(blob_file_path, blob_file_id, tree_id)?,

            results: Vec::new(),

            compression: CompressionType::None,
            passthrough_compression: CompressionType::None,

            tree_id,
            descriptor_table,
        })
    }

    /// Sets the blob file target size.
    #[must_use]
    pub fn use_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }

    /// Sets the compression method in blob file writer metadata, but does not actually compress blobs.
    ///
    /// This is used in garbage collection to pass through already-compressed blobs, but correctly
    /// set the compression type in the metadata.
    pub(crate) fn use_passthrough_compression(mut self, compression: CompressionType) -> Self {
        assert_eq!(self.compression, CompressionType::None);
        self.passthrough_compression = compression;
        self
    }

    /// Sets the compression method.
    #[must_use]
    #[doc(hidden)]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression.clone_from(&compression);
        self.active_writer.compression = compression;
        self
    }

    /// Sets up a new writer for the next blob file.
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating blob file writer");

        let new_blob_file_id = self.id_generator.next();
        let blob_file_path = self.folder.join(new_blob_file_id.to_string());

        let new_writer = Writer::new(blob_file_path, new_blob_file_id, self.tree_id)?
            .use_compression(self.compression);

        let old_writer = std::mem::replace(&mut self.active_writer, new_writer);
        let blob_file = Self::consume_writer(
            old_writer,
            self.passthrough_compression,
            self.descriptor_table.clone(),
        )?;
        self.results.extend(blob_file);

        Ok(())
    }

    fn consume_writer(
        writer: Writer,
        passthrough_compression: CompressionType,
        descriptor_table: Option<Arc<DescriptorTable>>,
    ) -> crate::Result<Option<BlobFile>> {
        if writer.item_count > 0 {
            let blob_file_id = writer.blob_file_id;
            let path = writer.path.clone();

            log::debug!(
                "Created blob file #{blob_file_id:?} ({} items, {} userdata bytes)",
                writer.item_count,
                writer.uncompressed_bytes,
            );

            let tree_id = writer.tree_id;

            let (metadata, checksum) = writer.finish()?;

            let file: Arc<dyn FsFile> = Arc::new(std::fs::File::open(&path)?);
            let file_accessor = descriptor_table.map_or_else(
                || FileAccessor::File(file.clone()),
                FileAccessor::DescriptorTable,
            );
            file_accessor.insert_for_blob_file((tree_id, blob_file_id).into(), file);

            let blob_file = BlobFile(Arc::new(BlobFileInner {
                checksum,
                tree_id,
                path,
                is_deleted: AtomicBool::new(false),
                id: blob_file_id,
                file_accessor,
                meta: Metadata {
                    id: blob_file_id,
                    version: metadata.version,
                    created_at: crate::time::unix_timestamp().as_nanos(),
                    item_count: metadata.item_count,
                    total_compressed_bytes: metadata.total_compressed_bytes,
                    total_uncompressed_bytes: metadata.total_uncompressed_bytes,
                    key_range: metadata.key_range.clone(),

                    compression: if passthrough_compression == CompressionType::None {
                        metadata.compression
                    } else {
                        passthrough_compression
                    },
                },
            }));

            Ok(Some(blob_file))
        } else {
            log::debug!(
                "Blob file writer at {} has written no data, deleting empty blob file",
                writer.path.display(),
            );

            if let Err(e) = std::fs::remove_file(&writer.path) {
                log::warn!(
                    "Could not delete empty blob file at {}: {e:?}",
                    writer.path.display(),
                );
            }

            Ok(None)
        }
    }

    /// Writes an item.
    ///
    /// Returns the [`ValueHandle`] of the written blob.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: &[u8], seqno: SeqNo, value: &[u8]) -> crate::Result<ValueHandle> {
        let target_size = self.target_size;

        // Write actual value into blob file
        let writer = &mut self.active_writer;

        let offset = writer.offset();
        let on_disk_value_len = writer.write(key, seqno, value)?;

        let handle = ValueHandle {
            blob_file_id: writer.blob_file_id(),
            offset,
            on_disk_size: on_disk_value_len,
        };

        // Check for blob file size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            self.rotate()?;
        }

        Ok(handle)
    }

    pub(crate) fn write_raw(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        uncompressed_len: u32,
    ) -> crate::Result<ValueHandle> {
        let target_size = self.target_size;

        // Write actual value into blob file
        let writer = &mut self.active_writer;

        let offset = writer.offset();
        let on_disk_value_len = writer.write_raw(key, seqno, value, uncompressed_len)?;

        let handle = ValueHandle {
            blob_file_id: writer.blob_file_id(),
            offset,
            on_disk_size: on_disk_value_len,
        };

        // Check for blob file size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            self.rotate()?;
        }

        Ok(handle)
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<BlobFile>> {
        let blob_file = Self::consume_writer(
            self.active_writer,
            self.passthrough_compression,
            self.descriptor_table.clone(),
        )?;
        self.results.extend(blob_file);
        Ok(self.results)
    }
}
