// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod report;

use crate::vlog::{BlobFileId, ValueLog};

/// GC strategy
#[allow(clippy::module_name_repetitions)]
pub trait GcStrategy {
    /// Picks blob files based on a predicate.
    fn pick(&self, value_log: &ValueLog) -> Vec<BlobFileId>;
}

/// Picks blob files that have a certain percentage of stale blobs
pub struct StaleThresholdStrategy(f32);

impl StaleThresholdStrategy {
    /// Creates a new strategy with the given threshold.
    ///
    /// # Panics
    ///
    /// Panics if the ratio is invalid.
    #[must_use]
    pub fn new(ratio: f32) -> Self {
        assert!(
            ratio.is_finite() && ratio.is_sign_positive(),
            "invalid stale ratio"
        );
        Self(ratio.min(1.0))
    }
}

impl GcStrategy for StaleThresholdStrategy {
    fn pick(&self, value_log: &ValueLog) -> Vec<BlobFileId> {
        unimplemented!()

        // value_log
        //     .manifest
        //     .blob_files
        //     .read()
        //     .expect("lock is poisoned")
        //     .values()
        //     .filter(|x| x.stale_ratio() > self.0)
        //     .map(|x| x.id)
        //     .collect::<Vec<_>>()
    }
}

/// Tries to find a least-effort-selection of blob files to merge to reach a certain space amplification
pub struct SpaceAmpStrategy(f32);

impl SpaceAmpStrategy {
    /// Creates a new strategy with the given space amp factor.
    ///
    /// # Panics
    ///
    /// Panics if the space amp factor is < 1.0.
    #[must_use]
    pub fn new(ratio: f32) -> Self {
        assert!(ratio >= 1.0, "invalid space amp ratio");
        Self(ratio)
    }
}

impl GcStrategy for SpaceAmpStrategy {
    #[allow(clippy::cast_precision_loss, clippy::significant_drop_tightening)]
    fn pick(&self, value_log: &ValueLog) -> Vec<BlobFileId> {
        unimplemented!()

        // let space_amp_target = self.0;
        // let current_space_amp = value_log.space_amp();

        // if current_space_amp < space_amp_target {
        //     log::trace!("Space amp is <= target {space_amp_target}, nothing to do");
        //     vec![]
        // } else {
        //     log::debug!("Selecting blob files to GC, space_amp_target={space_amp_target}");

        //     let lock = value_log
        //         .manifest
        //         .blob_files
        //         .read()
        //         .expect("lock is poisoned");

        //     let mut blob_files = lock
        //         .values()
        //         .filter(|x| x.stale_ratio() > 0.0)
        //         .collect::<Vec<_>>();

        //     // Sort by stale ratio descending
        //     blob_files.sort_by(|a, b| {
        //         b.stale_ratio()
        //             .partial_cmp(&a.stale_ratio())
        //             .unwrap_or(std::cmp::Ordering::Equal)
        //     });

        //     let mut selection = vec![];

        //     let mut total_bytes = value_log.manifest.total_bytes();
        //     let mut stale_bytes = value_log.manifest.stale_bytes();

        //     for blob_file in blob_files {
        //         let blob_file_stale_bytes = blob_file.gc_stats.stale_bytes();
        //         stale_bytes -= blob_file_stale_bytes;
        //         total_bytes -= blob_file_stale_bytes;

        //         selection.push(blob_file.id);

        //         let space_amp_after_gc =
        //             total_bytes as f32 / (total_bytes as f32 - stale_bytes as f32);

        //         log::debug!(
        //             "Selected blob file #{} for GC: will reduce space amp to {space_amp_after_gc}",
        //             blob_file.id,
        //         );

        //         if space_amp_after_gc <= space_amp_target {
        //             break;
        //         }
        //     }

        //     selection
        // }
    }
}
