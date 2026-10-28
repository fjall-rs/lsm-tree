mod refs;

use crate::{
    blob_tree::FragmentationMap,
    version::{VersionId, DEFAULT_LEVEL_COUNT},
    EditRecord, SeqNo, TreeType, VersionEdit,
};

pub use refs::{BlobFileRef, TableRef};

/// Raw version data (without table/blob file handles)
///
/// This is rebuilt on recovery and then "hydrated" back into a usable `Version`
/// for a [`VersionSetProvider`](crate::VersionSetProvider).
#[derive(Debug, PartialEq, Eq)]
pub struct RawVersionData {
    tree_type: TreeType,
    version_id: VersionId,
    version_seqno: SeqNo,
    table_ids: Vec<Vec<Vec<TableRef>>>,
    blob_file_ids: Vec<BlobFileRef>,
    gc_stats: FragmentationMap,
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
            tree_type: TreeType::Standard, // TODO: later: create EditRecord::SetTreeType
            version_id: 0,
            version_seqno: 0,
        }
    }

    /// Applies a version edit to this raw version.
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
                        level_runs.push(tables.clone());
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
                        destination_runs.push(moved);
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
                EditRecord::SetVersionSeqno(seqno) => {
                    self.version_seqno = *seqno;
                }
                EditRecord::UpdateFragMap(map) => {
                    map.clone().merge_into(&mut self.gc_stats);
                }
                EditRecord::Custom(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        blob_tree::FragmentationEntry, vlog::BlobFileId, Checksum, EditRecord, TableId, VersionEdit,
    };

    fn table(id: TableId, checksum: u128) -> TableRef {
        TableRef::new(id, Checksum::from_raw(checksum))
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
                tree_type: TreeType::Standard,
                version_id: 0,
                version_seqno: 0,
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
            tables: vec![table(1, 11), table(2, 12)],
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
            Some(&vec![vec![table(1, 11), table(2, 12)]]),
        );
        assert_eq!(raw.blob_file_ids, vec![blob_file(3, 13)]);
        assert_eq!(raw.version_seqno, 42);
        assert_eq!(raw.gc_stats, frag_map);
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
            EditRecord::Custom(vec![1, 2, 3]),
        ]));

        assert_eq!(
            raw.table_ids.first(),
            Some(&vec![vec![table(2, 102)]]),
            "moving tables should remove them from their original runs",
        );
        assert_eq!(raw.table_ids.get(1).map(Vec::len), Some(0));
        assert_eq!(
            raw.table_ids.get(2),
            Some(&vec![vec![table(1, 101), table(3, 103)]]),
            "moved tables should form one new run in source order",
        );
        assert_eq!(raw.blob_file_ids, vec![blob_file(5, 105)]);
        assert_eq!(raw.version_seqno, 7);
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
