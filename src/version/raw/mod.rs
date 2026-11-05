mod refs;

use crate::{
    blob_tree::FragmentationMap,
    version::{VersionId, DEFAULT_LEVEL_COUNT},
    EditRecord, SeqNo, VersionEdit,
};

pub use refs::{BlobFileRef, TableRef};

/// Raw version data (without table/blob file handles)
///
/// This is rebuilt on recovery and then "hydrated" back into a usable `Version`
/// for a [`VersionSetProvider`](crate::VersionSetProvider).
#[derive(Debug, PartialEq, Eq)]
pub struct RawVersionData {
    pub(crate) version_id: VersionId,
    pub(crate) version_seqno: SeqNo,
    pub(crate) flush_watermark: Option<SeqNo>,
    pub(crate) table_ids: Vec<Vec<Vec<TableRef>>>,
    pub(crate) blob_file_ids: Vec<BlobFileRef>,
    pub(crate) gc_stats: FragmentationMap,
}

impl Default for RawVersionData {
    fn default() -> Self {
        Self::new()
    }
}

impl RawVersionData {
    /// Creates an empty raw version with the standard number of levels.
    #[must_use]
    pub fn new() -> Self {
        Self {
            blob_file_ids: Vec::new(),
            gc_stats: FragmentationMap::default(),
            table_ids: (0..usize::from(DEFAULT_LEVEL_COUNT))
                .map(|_| Vec::new())
                .collect(),
            version_id: 0,
            version_seqno: 0,
            flush_watermark: None,
        }
    }

    /// Applies a version edit to this raw version.
    ///
    /// Like in [`crate::version::Version`], the runs of a level are ordered
    /// newest first, so added and moved runs are put at the front of their level.
    ///
    /// # Panics
    ///
    /// Panics when an add record contains a duplicate ID or refers to a level
    /// outside the fixed level count. Invalid drops and moves are logged and
    /// otherwise ignored.
    pub fn apply(&mut self, edit: &VersionEdit) {
        for record in edit.iter() {
            match record {
                EditRecord::AddTables { level, tables } => {
                    assert!(
                        *level < self.table_ids.len(),
                        "table level {level} is outside the fixed level count {}",
                        self.table_ids.len(),
                    );

                    for (index, table) in tables.iter().enumerate() {
                        assert!(
                            !self
                                .table_ids
                                .iter()
                                .flatten()
                                .flatten()
                                .any(|current| current.id == table.id),
                            "table ID {} already exists",
                            table.id,
                        );
                        assert!(
                            !tables
                                .iter()
                                .take(index)
                                .any(|previous| previous.id == table.id),
                            "table ID {} is duplicated in an AddTables edit",
                            table.id,
                        );
                    }

                    if !tables.is_empty() {
                        let Some(level_runs) = self.table_ids.get_mut(*level) else {
                            unreachable!("table level was validated above");
                        };
                        // NOTE: Newest run first, like in `Version`
                        level_runs.insert(0, tables.clone());
                    }
                }
                EditRecord::AddBlobFiles { blob_files } => {
                    for (index, blob_file) in blob_files.iter().enumerate() {
                        assert!(
                            !self
                                .blob_file_ids
                                .iter()
                                .any(|current| current.id == blob_file.id),
                            "blob file ID {} already exists",
                            blob_file.id,
                        );
                        assert!(
                            !blob_files
                                .iter()
                                .take(index)
                                .any(|previous| previous.id == blob_file.id),
                            "blob file ID {} is duplicated in an AddBlobFiles edit",
                            blob_file.id,
                        );
                    }

                    self.blob_file_ids.extend(blob_files.iter().copied());
                }
                EditRecord::DropTables(table_ids) => {
                    for table_id in table_ids {
                        let mut found = false;

                        for level in &mut self.table_ids {
                            for run in &mut *level {
                                let old_len = run.len();
                                run.retain(|table| table.id != *table_id);
                                found |= run.len() != old_len;
                            }
                            level.retain(|run| !run.is_empty());
                        }

                        if !found {
                            log::warn!("Tried to drop unknown table ID {table_id}");
                        }
                    }
                }
                EditRecord::MoveTables { dst_lvl, tables } => {
                    assert!(
                        *dst_lvl < self.table_ids.len(),
                        "table level {dst_lvl} is outside the fixed level count {}",
                        self.table_ids.len(),
                    );

                    let mut moved = Vec::new();

                    for level in &mut self.table_ids {
                        for run in &mut *level {
                            let mut retained = Vec::with_capacity(run.len());

                            for table in run.drain(..) {
                                if tables.contains(&table.id) {
                                    moved.push(table);
                                } else {
                                    retained.push(table);
                                }
                            }

                            *run = retained;
                        }
                        level.retain(|run| !run.is_empty());
                    }

                    for table_id in tables {
                        if !moved.iter().any(|table| table.id == *table_id) {
                            log::warn!("Tried to move unknown table ID {table_id}");
                        }
                    }

                    if !moved.is_empty() {
                        let Some(destination_runs) = self.table_ids.get_mut(*dst_lvl) else {
                            unreachable!("destination level was validated above");
                        };
                        // NOTE: Newest run first, like in `Version`
                        destination_runs.insert(0, moved);
                    }
                }
                EditRecord::DropBlobFiles(blob_file_ids) => {
                    for blob_file_id in blob_file_ids {
                        let old_len = self.blob_file_ids.len();
                        self.blob_file_ids
                            .retain(|blob_file| blob_file.id != *blob_file_id);

                        if self.blob_file_ids.len() == old_len {
                            log::warn!("Tried to drop unknown blob file ID {blob_file_id}");
                        }
                    }

                    self.gc_stats.retain(|blob_file_id, _| {
                        self.blob_file_ids
                            .iter()
                            .any(|blob_file| blob_file.id == *blob_file_id)
                    });
                }
                EditRecord::SetFlushWatermark(seqno) => {
                    self.flush_watermark = Some(*seqno);
                }
                EditRecord::SetVersionSeqno(seqno) => {
                    self.version_seqno = *seqno;
                }
                EditRecord::UpdateFragMap(map) => {
                    map.clone().merge_into(&mut self.gc_stats);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        blob_tree::FragmentationEntry,
        coding::{Decode, Encode},
        vlog::BlobFileId,
        Checksum, EditRecord, TableId, VersionEdit,
    };

    fn table(id: TableId, checksum: u128) -> TableRef {
        TableRef::new(id, Checksum::from_raw(checksum))
    }

    fn table_with_global_seqno(id: TableId, checksum: u128, global_seqno: SeqNo) -> TableRef {
        TableRef::with_global_seqno(id, Checksum::from_raw(checksum), global_seqno)
    }

    fn blob_file(id: BlobFileId, checksum: u128) -> BlobFileRef {
        BlobFileRef::new(id, Checksum::from_raw(checksum))
    }

    #[test]
    fn apply_empty_edit_keeps_empty_version() {
        let raw = RawVersionData::new();

        assert_eq!(
            raw,
            RawVersionData {
                version_id: 0,
                version_seqno: 0,
                flush_watermark: None,
                table_ids: (0..usize::from(DEFAULT_LEVEL_COUNT))
                    .map(|_| Vec::new())
                    .collect(),
                blob_file_ids: Vec::new(),
                gc_stats: FragmentationMap::default(),
            },
        );
    }

    #[test]
    fn apply_single_record_edits() {
        let mut raw = RawVersionData::new();

        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: vec![table_with_global_seqno(1, 11, 7), table(2, 12)],
        }]));
        raw.apply(&VersionEdit::new(vec![EditRecord::AddBlobFiles {
            blob_files: vec![blob_file(3, 13)],
        }]));
        raw.apply(&VersionEdit::new(vec![EditRecord::SetVersionSeqno(42)]));

        let mut frag_map = FragmentationMap::default();
        frag_map.insert(3, FragmentationEntry::new(1, 2, 3));
        raw.apply(&VersionEdit::new(vec![EditRecord::UpdateFragMap(
            frag_map.clone(),
        )]));

        assert_eq!(
            raw.table_ids.first(),
            Some(&vec![
                vec![table_with_global_seqno(1, 11, 7), table(2, 12),]
            ]),
        );
        assert_eq!(raw.blob_file_ids, vec![blob_file(3, 13)]);
        assert_eq!(raw.version_seqno, 42);
        assert_eq!(raw.gc_stats, frag_map);
    }

    #[test]
    fn table_global_seqno_roundtrips_in_version_edit() {
        let edit = VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: vec![table_with_global_seqno(1, 11, 7)],
        }]);

        let encoded = edit.encode_into_vec();
        let decoded = VersionEdit::decode_from(&mut &encoded[..]).expect("should decode edit");

        assert_eq!(edit, decoded);
    }

    #[test]
    fn flush_watermark_roundtrips_in_version_edit() {
        for seqno in [0, 1, 127, 128, u64::MAX] {
            let edit = VersionEdit::new(vec![
                EditRecord::SetFlushWatermark(seqno),
                EditRecord::SetVersionSeqno(seqno),
            ]);

            let encoded = edit.encode_into_vec();
            let decoded = VersionEdit::decode_from(&mut &encoded[..]).expect("should decode edit");

            assert_eq!(edit, decoded);
        }
    }

    #[test]
    fn apply_sets_flush_watermark() {
        let mut raw = RawVersionData::new();
        assert_eq!(None, raw.flush_watermark);

        // NOTE: Seqno 0 is a valid watermark (Some(0)), not "nothing flushed"
        raw.apply(&VersionEdit::new(vec![EditRecord::SetFlushWatermark(0)]));
        assert_eq!(Some(0), raw.flush_watermark);

        raw.apply(&VersionEdit::new(vec![EditRecord::SetFlushWatermark(20)]));
        assert_eq!(Some(20), raw.flush_watermark);

        // Edits without a watermark (e.g. compactions) keep it
        raw.apply(&VersionEdit::new(vec![
            EditRecord::AddTables {
                level: 6,
                tables: vec![table(1, 101)],
            },
            EditRecord::SetVersionSeqno(30),
        ]));
        assert_eq!(Some(20), raw.flush_watermark);
        assert_eq!(30, raw.version_seqno);
    }

    #[test]
    fn apply_multiple_edits_with_varying_lengths() {
        let mut raw = RawVersionData::new();

        raw.apply(&VersionEdit::empty());
        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: Vec::new(),
        }]));
        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: vec![table(1, 101)],
        }]));
        raw.apply(&VersionEdit::new(vec![
            EditRecord::AddTables {
                level: 0,
                tables: vec![table(2, 102), table(3, 103)],
            },
            EditRecord::AddTables {
                level: 1,
                tables: vec![table(4, 104)],
            },
            EditRecord::AddBlobFiles {
                blob_files: vec![blob_file(5, 105), blob_file(6, 106)],
            },
            EditRecord::SetVersionSeqno(7),
        ]));
        raw.apply(&VersionEdit::new(vec![
            EditRecord::MoveTables {
                dst_lvl: 2,
                tables: vec![1, 3],
            },
            EditRecord::DropTables(vec![4, 999]),
            EditRecord::DropBlobFiles(vec![6, 999]),
        ]));

        assert_eq!(
            raw.table_ids.first(),
            Some(&vec![vec![table(2, 102)]]),
            "moving tables should remove them from their original runs",
        );
        assert_eq!(raw.table_ids.get(1).map(Vec::len), Some(0));
        assert_eq!(
            raw.table_ids.get(2),
            // NOTE: Like `Version::with_moved`, moved tables are collected in version order
            // (newest run first), and table #3 is in the newer L0 run
            Some(&vec![vec![table(3, 103), table(1, 101)]]),
            "moved tables should form one new run in version order",
        );
        assert_eq!(raw.blob_file_ids, vec![blob_file(5, 105)]);
        assert_eq!(raw.version_seqno, 7);
    }

    #[test]
    fn apply_puts_new_runs_first() {
        let mut raw = RawVersionData::new();

        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: vec![table(1, 101)],
        }]));
        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: vec![table(2, 102)],
        }]));

        assert_eq!(
            raw.table_ids.first(),
            Some(&vec![vec![table(2, 102)], vec![table(1, 101)]]),
            "newer L0 run should be first",
        );

        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 1,
            tables: vec![table(3, 103)],
        }]));
        raw.apply(&VersionEdit::new(vec![EditRecord::MoveTables {
            dst_lvl: 1,
            tables: vec![1],
        }]));

        assert_eq!(
            raw.table_ids.get(1),
            Some(&vec![vec![table(1, 101)], vec![table(3, 103)]]),
            "moved run should be first in its destination level",
        );
    }

    #[test]
    #[should_panic(expected = "already exists")]
    fn adding_duplicate_table_panics() {
        let mut raw = RawVersionData::new();
        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 0,
            tables: vec![table(1, 11)],
        }]));
        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: 1,
            tables: vec![table(1, 12)],
        }]));
    }

    #[test]
    #[should_panic(expected = "fixed level count")]
    fn adding_tables_beyond_fixed_levels_panics() {
        let mut raw = RawVersionData::new();
        raw.apply(&VersionEdit::new(vec![EditRecord::AddTables {
            level: usize::from(DEFAULT_LEVEL_COUNT),
            tables: vec![table(1, 11)],
        }]));
    }
}
