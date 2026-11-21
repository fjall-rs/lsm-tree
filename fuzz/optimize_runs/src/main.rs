#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::{KeyRange, Ranged, Run, optimize_runs};
use std::collections::BTreeMap;

#[derive(Arbitrary, Debug)]
enum Operation {
    Insert { key: u8, seqno: u64 },
    Flush,
}

type Table = BTreeMap<u8, u64>;
type Runs = Vec<Run<FuzzTable>>;

#[derive(Clone, Debug)]
struct FuzzTable {
    id: usize,
    key_range: KeyRange,
}

impl Ranged for FuzzTable {
    fn key_range(&self) -> &KeyRange {
        &self.key_range
    }
}

#[derive(Default)]
struct FuzzState {
    buffer: Table,
    runs: Runs,
    tables: Vec<Table>,
    expected: Table,
}

impl FuzzState {
    fn insert(&mut self, key: u8, seqno: u64) {
        self.buffer.insert(key, seqno);
        self.expected.insert(key, seqno);
    }

    fn verify(&self) {
        let mut table_ids = self
            .runs
            .iter()
            .flat_map(|run| run.iter().map(|table| table.id))
            .collect::<Vec<_>>();
        table_ids.sort_unstable();
        assert_eq!(table_ids, (0..self.tables.len()).collect::<Vec<_>>());

        for run in &self.runs {
            for adjacent in run.windows(2) {
                assert!(
                    adjacent[0].key_range.max() < adjacent[1].key_range.min(),
                    "optimized run is not sorted and disjoint: {run:?}"
                );
            }
        }

        for (&key, &expected_seqno) in &self.expected {
            let actual = self.runs.iter().find_map(|run| {
                run.get_for_key(&[key])
                    .and_then(|table| self.tables[table.id].get(&key).copied())
            });

            assert_eq!(
                actual,
                Some(expected_seqno),
                "wrong visible version for key {key}: runs={:?}",
                self.runs
            );
        }
    }

    fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let min = *self
                .buffer
                .first_key_value()
                .expect("buffer is not empty")
                .0;
            let max = *self.buffer.last_key_value().expect("buffer is not empty").0;
            let id = self.tables.len();

            self.tables.push(std::mem::take(&mut self.buffer));
            self.runs.insert(
                0,
                Run::new(vec![FuzzTable {
                    id,
                    key_range: KeyRange::new((vec![min].into(), vec![max].into())),
                }])
                .expect("flushed table is not empty"),
            );
            self.runs = optimize_runs(std::mem::take(&mut self.runs));
        }

        self.verify();
    }
}

fn run_operations(operations: impl IntoIterator<Item = Operation>) {
    let mut state = FuzzState::default();

    for operation in operations.into_iter().take(256) {
        match operation {
            Operation::Insert { key, seqno } => state.insert(key, seqno),
            Operation::Flush => state.flush(),
        }
    }

    state.flush();
}

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);
        let Ok(operations) = unstructured.arbitrary_iter::<Operation>() else {
            return;
        };
        let Ok(operations) = operations.take(256).collect::<arbitrary::Result<Vec<_>>>() else {
            return;
        };

        run_operations(operations);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(min: u8, max: u8) -> KeyRange {
        KeyRange::new((vec![min].into(), vec![max].into()))
    }

    #[test]
    #[should_panic(expected = "not sorted and disjoint")]
    fn verify_rejects_unsorted_disjoint_ranges() {
        let tables = vec![BTreeMap::from([(b'a', 1)]), BTreeMap::from([(b'z', 2)])];
        let expected = BTreeMap::from([(b'a', 1), (b'z', 2)]);
        let runs = vec![
            Run::new(vec![
                FuzzTable {
                    id: 1,
                    key_range: range(b'z', b'z'),
                },
                FuzzTable {
                    id: 0,
                    key_range: range(b'a', b'a'),
                },
            ])
            .unwrap(),
        ];

        FuzzState {
            runs,
            tables,
            expected,
            ..FuzzState::default()
        }
        .verify();
    }

    #[test]
    fn empty_and_repeated_flushes_preserve_state() {
        let mut state = FuzzState::default();
        state.flush();
        assert!(state.tables.is_empty());
        assert!(state.runs.is_empty());

        state.insert(b'a', 1);
        state.insert(b'a', 2);
        state.flush();
        state.flush();

        assert!(state.buffer.is_empty());
        assert_eq!(state.tables, vec![BTreeMap::from([(b'a', 2)])]);
        assert_eq!(state.expected, BTreeMap::from([(b'a', 2)]));
        assert_eq!(state.runs.len(), 1);
    }

    #[test]
    fn boundary_keys_remain_visible_across_disjoint_tables_and_gaps() {
        run_operations([
            Operation::Insert {
                key: b'a',
                seqno: 0,
            },
            Operation::Insert {
                key: b'd',
                seqno: 0,
            },
            Operation::Flush,
            Operation::Insert {
                key: b'm',
                seqno: 1,
            },
            Operation::Insert {
                key: b'p',
                seqno: 1,
            },
            Operation::Flush,
            Operation::Insert {
                key: b'c',
                seqno: 2,
            },
            Operation::Insert {
                key: b'n',
                seqno: 2,
            },
            Operation::Flush,
        ]);
    }

    #[test]
    fn transitive_overlap_keeps_the_newest_value_visible() {
        // Oldest [a, c], middle [a, z], newest [m, p]. The newest table is disjoint from the
        // oldest but overlaps the middle, so moving it behind the middle would expose seqno 1.
        run_operations([
            Operation::Insert {
                key: b'a',
                seqno: 0,
            },
            Operation::Insert {
                key: b'c',
                seqno: 0,
            },
            Operation::Flush,
            Operation::Insert {
                key: b'a',
                seqno: 1,
            },
            Operation::Insert {
                key: b'm',
                seqno: 1,
            },
            Operation::Insert {
                key: b'z',
                seqno: 1,
            },
            Operation::Flush,
            Operation::Insert {
                key: b'm',
                seqno: 2,
            },
            Operation::Insert {
                key: b'p',
                seqno: 2,
            },
            Operation::Flush,
        ]);
    }
}
