//! Contains compaction strategies

pub(crate) mod fifo;
pub(crate) mod levelled;
pub(crate) mod maintenance;
pub(crate) mod major;
pub(crate) mod pulldown;
pub(crate) mod tiered;
pub(crate) mod worker;

use crate::{config::Config, levels::LevelManifest, segment::meta::SegmentId};

pub use fifo::Strategy as Fifo;
pub use levelled::Strategy as Levelled;
pub use tiered::Strategy as SizeTiered;

/// Alias for `Levelled`
pub type Leveled = Levelled;

#[doc(hidden)]
pub use pulldown::Strategy as PullDown;

/// Input for compactor.
///
/// The compaction strategy chooses which segments to compact and how.
/// That information is given to the compactor.
#[derive(Debug, Eq, PartialEq)]
pub struct Input {
    /// Segments to compact
    pub segment_ids: Vec<SegmentId>,

    /// Level to put the created segments into
    pub dest_level: u8,

    /// Segment target size
    ///
    /// If a segment compaction reaches the level, a new segment is started.
    /// This results in a sorted "run" of segments
    pub target_size: u64,
}

/// Describes what to do (compact or not)
#[derive(Debug, Eq, PartialEq)]
pub enum Choice {
    /// Just do nothing.
    DoNothing,

    /// Moves segments into another level without rewriting.
    Move(Input),

    /// Compacts some segments into a new level.
    Merge(Input),

    /// Delete segments without doing compaction.
    ///
    /// This may be used by a compaction strategy that wants to delete old data
    /// without having to compact it away, like [`fifo::Strategy`].
    Drop(Vec<SegmentId>),
}

/// Trait for a compaction strategy
///
/// The strategy receives the levels of the LSM-tree as argument
/// and emits a choice on what to do.
#[allow(clippy::module_name_repetitions)]
pub trait CompactionStrategy {
    /// Decides on what to do based on the current state of the LSM-tree's levels
    fn choose(&self, _: &LevelManifest, config: &Config) -> Choice;
}
