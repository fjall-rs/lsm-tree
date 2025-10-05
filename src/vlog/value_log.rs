// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    // file::VLOG_MARKER,
    vlog::{
        blob_file::{
            gc_stats::GcStats, merge::MergeReader, meta::Metadata, Inner as BlobFileInner,
        },
        gc::report::GcReport,
        index::Writer as IndexWriter,
        scanner::SizeMap,
        BlobFile, BlobFileId, BlobFileWriter, GcStrategy, IndexReader, ValueHandle,
    },
    Cache,
    DescriptorTable,
    KeyRange,
    UserValue,
};
use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Mutex},
};

// // TODO: use other counter struct
// #[allow(clippy::module_name_repetitions)]
// #[derive(Clone, Default)]
// pub struct IdGenerator(Arc<AtomicU64>);

// impl std::ops::Deref for IdGenerator {
//     type Target = Arc<AtomicU64>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl IdGenerator {
//     pub fn new(start: u64) -> Self {
//         Self(Arc::new(AtomicU64::new(start)))
//     }

//     pub fn next(&self) -> BlobFileId {
//         self.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
//     }
// }

fn unlink_blob_files(base_path: &Path, ids: &[BlobFileId]) {
    unimplemented!()

    // for id in ids {
    //     let path = base_path.join(BLOB_FILES_FOLDER).join(id.to_string());

    //     if let Err(e) = std::fs::remove_file(&path) {
    //         log::error!("Could not free blob file at {path:?}: {e:?}");
    //     }
    // }
}

/// A disk-resident value log
#[derive(Clone)]
pub struct ValueLog(Arc<ValueLogInner>);

impl std::ops::Deref for ValueLog {
    type Target = ValueLogInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct ValueLogInner {
    /// Base folder
    pub path: PathBuf,

    /// Value log configuration
    // config: Config,

    /// In-memory blob cache
    // blob_cache: Arc<Cache>,

    /// In-memory FD cache
    // fd_cache: Arc<DescriptorTable>,

    // /// Generator to get next blob file ID
    // id_generator: IdGenerator,

    /// Guards the rollover (compaction) process to only
    /// allow one to happen at a time
    #[doc(hidden)]
    pub rollover_guard: Mutex<()>,
}

impl ValueLog {
    /// Creates or recovers a value log in the given directory.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open<P: Into<PathBuf>>(
        path: P, // TODO: move path into config?
                 // config: Config,
    ) -> crate::Result<Self> {
        // let path = path.into();

        // if path.join(VLOG_MARKER).try_exists()? {
        //     Self::recover(path, config)
        // } else {
        //     Self::create_new(path, config)
        // }

        unimplemented!()
    }

    /* /// Prints fragmentation histogram.
    pub fn print_fragmentation_histogram(&self) {
        let lock = self.manifest.blob_files.read().expect("lock is poisoned");

        for (id, blob_file) in &*lock {
            let stale_ratio = blob_file.stale_ratio();

            let progress = (stale_ratio * 10.0) as usize;
            let void = 10 - progress;

            let progress = "=".repeat(progress);
            let void = " ".repeat(void);

            println!(
                "{id:0>4} [{progress}{void}] {}%",
                (stale_ratio * 100.0) as usize
            );
        }
    } */

    #[doc(hidden)]
    pub fn verify(&self) -> crate::Result<usize> {
        unimplemented!()

        // let _lock = self.rollover_guard.lock().expect("lock is poisoned");

        // let mut sum = 0;

        // for item in self.get_reader()? {
        //     let (k, v, _, expected_checksum) = item?;

        //     let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        //     hasher.update(&k);
        //     hasher.update(&v);

        //     if hasher.digest() != expected_checksum {
        //         sum += 1;
        //     }
        // }

        // Ok(sum)
    }

    /// Creates a new empty value log in a directory.
    pub(crate) fn create_new<P: Into<PathBuf>>(path: P) -> crate::Result<Self> {
        let path = path.into();

        let path = crate::path::absolute_path(&path);
        log::trace!("Creating value-log at {}", path.display());

        std::fs::create_dir_all(&path)?;

        // let marker_path = path.join(VLOG_MARKER);
        // assert!(!marker_path.try_exists()?);

        // NOTE: Lastly, fsync .vlog marker, which contains the version
        // -> the V-log is fully initialized

        // let mut file = std::fs::File::create(marker_path)?;
        // FormatVersion::V3.write_file_header(&mut file)?;
        // file.sync_all()?;

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix

            let folder = std::fs::File::open(&path)?;
            folder.sync_all()?;
        }

        // let blob_cache = config.blob_cache.clone();
        // let fd_cache = config.fd_cache.clone();
        // let manifest = Manifest::create_new(&path)?;

        Ok(Self(Arc::new(ValueLogInner {
            // config,
            path,
            // blob_cache,
            // fd_cache,
            // manifest,
            // id_generator: IdGenerator::default(),
            rollover_guard: Mutex::new(()),
        })))
    }

    /// Returns the number of blob files in the value log.
    #[must_use]
    pub fn blob_file_count(&self) -> usize {
        unimplemented!()

        // self.manifest.len()
    }

    /// Resolves a value handle.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get(&self, vhandle: &ValueHandle) -> crate::Result<Option<UserValue>> {
        self.get_with_prefetch(vhandle, 0)
    }

    /// Resolves a value handle, and prefetches some values after it.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get_with_prefetch(
        &self,
        vhandle: &ValueHandle,
        prefetch_size: usize,
    ) -> crate::Result<Option<UserValue>> {
        // TODO:, first rewrite blob files to use pread
        Ok(None)

        // if let Some(value) = self.blob_cache.get(self.id, vhandle) {
        //     return Ok(Some(value));
        // }

        // let Some(blob_file) = self.manifest.get_blob_file(vhandle.blob_file_id) else {
        //     return Ok(None);
        // };

        // // TODO: get FD from cache or open and insert
        // // let mut reader = match self
        // //     .fd_cache
        // //     .access_for_blob_file(&GlobalSegmentId::from((self.id, vhandle.blob_file_id)))
        // // {
        // //     Some(fd) => fd,
        // //     None => BufReader::new(File::open(&blob_file.path)?),
        // // };

        // let mut reader = BlobFileReader::with_reader(vhandle.blob_file_id, reader)
        //     .use_compression(self.config.compression.clone());

        // let Some(item) = reader.next() else {
        //     return Ok(None);
        // };
        // let (_key, val, _checksum) = item?;

        // self.blob_cache.insert(self.id, vhandle, val.clone());

        // // TODO: maybe we can look at the value size and prefetch some more values
        // // without causing another I/O...
        // // TODO: benchmark range reads for rather small non-inlined blobs (maybe ~512-1000B)
        // // and see how different BufReader capacities and prefetch changes range read performance
        // for _ in 0..prefetch_size {
        //     let offset = reader.get_offset()?;

        //     let Some(item) = reader.next() else {
        //         break;
        //     };
        //     let (_key, val, _checksum) = item?;

        //     let value_handle = ValueHandle {
        //         blob_file_id: vhandle.blob_file_id,
        //         offset,
        //     };

        //     self.blob_cache.insert(self.id, &value_handle, val);
        // }

        // Ok(Some(val))
    }

    fn get_writer_raw(&self) -> crate::Result<BlobFileWriter> {
        unimplemented!()

        // BlobFileWriter::new(
        //     self.id_generator.clone(),
        //     self.config.blob_file_size_bytes,
        //     &self.path,
        // )
        // .map_err(Into::into)
    }

    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get_writer(&self) -> crate::Result<BlobFileWriter> {
        unimplemented!()

        // self.get_writer_raw()
        //     .map(|x| x.use_compression(self.config.compression))
    }

    /// Drops stale blob files.
    ///
    /// Returns the amount of disk space (compressed data) freed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn drop_stale_blob_files(&self) -> crate::Result<u64> {
        unimplemented!()

        // // IMPORTANT: Only allow 1 rollover or GC at any given time
        // let _guard = self.rollover_guard.lock().expect("lock is poisoned");

        // let blob_files = self
        //     .manifest
        //     .blob_files
        //     .read()
        //     .expect("lock is poisoned")
        //     .values()
        //     .filter(|x| x.is_stale())
        //     .cloned()
        //     .collect::<Vec<_>>();

        // let bytes_freed = blob_files.iter().map(|x| x.meta.compressed_bytes).sum();

        // let ids = blob_files.iter().map(|x| x.id).collect::<Vec<_>>();

        // if ids.is_empty() {
        //     log::trace!("No blob files to drop");
        // } else {
        //     log::info!("Dropping stale blob files: {ids:?}");
        //     self.manifest.drop_blob_files(&ids)?;

        //     for blob_file in blob_files {
        //         std::fs::remove_file(&blob_file.path)?;
        //     }
        // }

        // Ok(bytes_freed)
    }

    /// Marks some blob files as stale.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn mark_as_stale(&self, ids: &[BlobFileId]) {
        unimplemented!()

        // // NOTE: Read-locking is fine because we are dealing with an atomic bool
        // #[allow(clippy::significant_drop_tightening)]
        // let blob_files = self.manifest.blob_files.read().expect("lock is poisoned");

        // for id in ids {
        //     let Some(blob_file) = blob_files.get(id) else {
        //         continue;
        //     };

        //     blob_file.mark_as_stale();
        // }
    }

    // TODO: remove?
    /// Returns the approximate space amplification.
    ///
    /// Returns 0.0 if there are no items.
    #[must_use]
    pub fn space_amp(&self) -> f32 {
        unimplemented!()

        // self.manifest.space_amp()
    }

    #[doc(hidden)]
    #[allow(clippy::cast_precision_loss)]
    #[must_use]
    pub fn consume_scan_result(&self, size_map: &SizeMap) -> GcReport {
        unimplemented!()

        // let mut report = GcReport {
        //     path: self.path.clone(),
        //     blob_file_count: self.blob_file_count(),
        //     stale_blob_file_count: 0,
        //     stale_bytes: 0,
        //     total_bytes: 0,
        //     stale_blobs: 0,
        //     total_blobs: 0,
        // };

        // for (&id, counter) in size_map {
        //     let blob_file = self
        //         .manifest
        //         .get_blob_file(id)
        //         .expect("blob file should exist");

        //     let total_bytes = blob_file.meta.total_uncompressed_bytes;
        //     let total_items = blob_file.meta.item_count;

        //     report.total_bytes += total_bytes;
        //     report.total_blobs += total_items;

        //     if counter.item_count > 0 {
        //         let used_size = counter.size;
        //         let alive_item_count = counter.item_count;

        //         let blob_file = self
        //             .manifest
        //             .get_blob_file(id)
        //             .expect("blob file should exist");

        //         let stale_bytes = total_bytes - used_size;
        //         let stale_items = total_items - alive_item_count;

        //         blob_file.gc_stats.set_stale_bytes(stale_bytes);
        //         blob_file.gc_stats.set_stale_items(stale_items);

        //         report.stale_bytes += stale_bytes;
        //         report.stale_blobs += stale_items;
        //     } else {
        //         log::debug!(
        //         "Blob file #{id} has no incoming references - can be dropped, freeing {} KiB on disk (userdata={} MiB)",
        //         blob_file.meta.compressed_bytes / 1_024,
        //         total_bytes / 1_024 / 1_024,
        //     );
        //         self.mark_as_stale(&[id]);

        //         report.stale_blob_file_count += 1;
        //         report.stale_bytes += total_bytes;
        //         report.stale_blobs += total_items;
        //     }
        // }

        // report
    }

    /// Scans the given index and collects GC statistics.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::significant_drop_tightening)]
    pub fn scan_for_stats(
        &self,
        iter: impl Iterator<Item = std::io::Result<(ValueHandle, u32)>>,
    ) -> crate::Result<GcReport> {
        unimplemented!()

        // let lock_guard = self.rollover_guard.lock().expect("lock is poisoned");

        // let ids = self.manifest.list_blob_file_ids();

        // let mut scanner = Scanner::new(iter, lock_guard, &ids);
        // scanner.scan()?;
        // let size_map = scanner.finish();
        // let report = self.consume_scan_result(&size_map);

        // Ok(report)
    }

    #[doc(hidden)]
    pub fn get_reader(&self) -> crate::Result<MergeReader> {
        unimplemented!()

        // let readers = self
        //     .manifest
        //     .blob_files
        //     .read()
        //     .expect("lock is poisoned")
        //     .values()
        //     .map(|x| x.scan())
        //     .collect::<crate::Result<Vec<_>>>()?;

        // Ok(MergeReader::new(readers))
    }

    /// Returns the amount of disk space (compressed data) freed.
    #[doc(hidden)]
    pub fn major_compact<R: IndexReader, W: IndexWriter>(
        &self,
        index_reader: &R,
        index_writer: W,
    ) -> crate::Result<u64> {
        unimplemented!()

        // let ids = self.manifest.list_blob_file_ids();
        // self.rollover(&ids, index_reader, index_writer)
    }

    /// Applies a GC strategy.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn apply_gc_strategy<R: IndexReader, W: IndexWriter>(
        &self,
        strategy: &impl GcStrategy,
        index_reader: &R,
        index_writer: W,
    ) -> crate::Result<u64> {
        unimplemented!()

        // let blob_file_ids = strategy.pick(self);
        // self.rollover(&blob_file_ids, index_reader, index_writer)
    }

    /// Atomically removes all data from the value log.
    ///
    /// If `prune_async` is set to `true`, the blob files will be removed from disk in a thread to avoid blocking.
    pub fn clear(&self, prune_async: bool) -> crate::Result<()> {
        unimplemented!()

        // let guard = self.rollover_guard.lock().expect("lock is poisoned");
        // let ids = self.manifest.list_blob_file_ids();
        // self.manifest.clear()?;
        // drop(guard);

        // if prune_async {
        //     let path = self.path.clone();

        //     std::thread::spawn(move || {
        //         log::trace!("Pruning dropped blob files in thread: {ids:?}");
        //         unlink_blob_files(&path, &ids);
        //         log::trace!("Successfully pruned all blob files");
        //     });
        // } else {
        //     log::trace!("Pruning dropped blob files: {ids:?}");
        //     unlink_blob_files(&self.path, &ids);
        //     log::trace!("Successfully pruned all blob files");
        // }

        // Ok(())
    }

    /// Rewrites some blob files into new blob files, blocking the caller
    /// until the operation is completely done.
    ///
    /// Returns the amount of disk space (compressed data) freed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn rollover<R: IndexReader, W: IndexWriter>(
        &self,
        ids: &[u64],
        index_reader: &R,
        mut index_writer: W,
    ) -> crate::Result<u64> {
        unimplemented!()

        // if ids.is_empty() {
        //     return Ok(0);
        // }

        // // IMPORTANT: Only allow 1 rollover or GC at any given time
        // let _guard = self.rollover_guard.lock().expect("lock is poisoned");

        // let size_before = self.manifest.disk_space_used();

        // log::info!("Rollover blob files {ids:?}");

        // let blob_files = ids
        //     .iter()
        //     .map(|&x| self.manifest.get_blob_file(x))
        //     .collect::<Option<Vec<_>>>();

        // let Some(blob_files) = blob_files else {
        //     return Ok(0);
        // };

        // let readers = blob_files
        //     .into_iter()
        //     .map(|x| x.scan())
        //     .collect::<crate::Result<Vec<_>>>()?;

        // // TODO: 3.0.0: Store uncompressed size per blob
        // // so we can avoid recompression costs during GC
        // // but have stats be correct

        // let reader = MergeReader::new(
        //     readers
        //         .into_iter()
        //         .map(|x| x.use_compression(self.config.compression.clone()))
        //         .collect(),
        // );

        // let mut writer = self
        //     .get_writer_raw()?
        //     .use_compression(self.config.compression.clone());

        // for item in reader {
        //     let (k, v, blob_file_id, _) = item?;

        //     match index_reader.get(&k)? {
        //         // If this value is in an older blob file, we can discard it
        //         Some(vhandle) if blob_file_id < vhandle.blob_file_id => continue,
        //         None => continue,
        //         _ => {}
        //     }

        //     let vhandle = writer.get_next_value_handle();

        //     // NOTE: Truncation is OK because we know values are u32 max
        //     #[allow(clippy::cast_possible_truncation)]
        //     index_writer.insert_indirect(&k, vhandle, v.len() as u32)?;

        //     writer.write(&k, &v)?;
        // }

        // // IMPORTANT: New blob files need to be persisted before adding to index
        // // to avoid dangling pointers
        // self.manifest.register(writer)?;

        // // NOTE: If we crash here, it's fine, the blob files are registered
        // // but never referenced, so they can just be dropped after recovery
        // index_writer.finish()?;

        // // IMPORTANT: We only mark the blob files as definitely stale
        // // The external index needs to decide when it is safe to drop
        // // the old blob files, as some reads may still be performed
        // self.mark_as_stale(ids);

        // let size_after = self.manifest.disk_space_used();

        // Ok(size_before.saturating_sub(size_after))
    }
}
