use crate::{
    blob_tree::handle::BlobIndirection,
    file::BLOBS_FOLDER,
    table::Table,
    tree::ingest::Ingestion as TableIngestion,
    vlog::{BlobFileWriter, ValueHandle},
    SeqNo, UserKey, UserValue,
};

/// Bulk ingestion for [`BlobTree`]
///
/// Items NEED to be added in ascending key order.
///
/// Uses table ingestion for the index and a blob file writer for large
/// values so both streams advance together.
pub struct BlobIngestion<'a> {
    tree: &'a crate::BlobTree,
    pub(crate) table: TableIngestion<'a>,
    pub(crate) blob: BlobFileWriter,
    seqno: SeqNo,
    separation_threshold: u32,
    last_key: Option<UserKey>,
}

impl<'a> BlobIngestion<'a> {
    /// Creates a new ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new(tree: &'a crate::BlobTree) -> crate::Result<Self> {
        let kv = tree
            .index
            .config
            .kv_separation_opts
            .as_ref()
            .expect("kv separation options should exist");

        let blob_file_size = kv.file_target_size;

        let table = TableIngestion::new(&tree.index)?;
        let blob = BlobFileWriter::new(
            tree.index.0.blob_file_id_counter.clone(),
            blob_file_size,
            tree.index.config.path.join(BLOBS_FOLDER),
        )?
        .use_compression(kv.compression);

        let separation_threshold = kv.separation_threshold;

        Ok(Self {
            tree,
            table,
            blob,
            seqno: 0,
            separation_threshold,
            last_key: None,
        })
    }

    /// Sets the ingestion seqno.
    #[must_use]
    pub fn with_seqno(mut self, seqno: SeqNo) -> Self {
        self.seqno = seqno;
        self.table = self.table.with_seqno(seqno);
        self
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        // Check order before any blob I/O to avoid partial writes on failure
        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        #[allow(clippy::cast_possible_truncation)]
        let value_size = value.len() as u32;

        if value_size >= self.separation_threshold {
            let offset = self.blob.offset();
            let blob_file_id = self.blob.blob_file_id();
            let on_disk_size = self.blob.write(&key, self.seqno, &value)?;

            let indirection = BlobIndirection {
                vhandle: ValueHandle {
                    blob_file_id,
                    offset,
                    on_disk_size,
                },
                size: value_size,
            };

            let cloned_key = key.clone();
            let res = self.table.write_indirection(key, indirection);
            if res.is_ok() {
                self.last_key = Some(cloned_key);
            }
            res
        } else {
            let cloned_key = key.clone();
            let res = self.table.write(key, value);
            if res.is_ok() {
                self.last_key = Some(cloned_key);
            }
            res
        }
    }

    /// Writes a tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        let cloned_key = key.clone();
        let res = self.table.write_tombstone(key);
        if res.is_ok() {
            self.last_key = Some(cloned_key);
        }
        res
    }

    /// Finishes the ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn finish(self) -> crate::Result<()> {
        use crate::AbstractTree;

        // Capture required handles before consuming fields during finalization
        let index = self.index().clone();
        let tree = self.tree.clone();

        // Finalize both value log and index writer so the index sees a
        // consistent set of blob files.
        let blob_files = self.blob.finish()?;
        let results = self.table.writer.finish()?;

        let created_tables = results
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                // Do not pin ingestion output tables here. Large ingests are
                // typically placed in level 1 and would otherwise keep all
                // filter and index blocks pinned, increasing memory pressure.
                Table::recover(
                    index
                        .config
                        .path
                        .join(crate::file::TABLES_FOLDER)
                        .join(table_id.to_string()),
                    checksum,
                    index.id,
                    index.config.cache.clone(),
                    index.config.descriptor_table.clone(),
                    false,
                    false,
                    #[cfg(feature = "metrics")]
                    index.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Blob ingestion only appends new tables and blob files; sealed
        // memtables remain unchanged and GC watermark stays at its
        // neutral value for this operation.
        tree.register_tables(&created_tables, Some(&blob_files), None, &[], 0)?;

        Ok(())
    }

    #[inline]
    fn index(&self) -> &crate::Tree {
        &self.tree.index
    }
}
