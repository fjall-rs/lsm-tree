#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::{KeyRange, fuzzing::optimize_runs};
use std::collections::BTreeMap;

#[derive(Arbitrary, Debug)]
enum Operation {
    Insert { key: u8, seqno: u64 },
    Flush,
}

type Table = BTreeMap<u8, u64>;
type Runs = Vec<Vec<(usize, KeyRange)>>;

fn verify(runs: &Runs, tables: &[Table], expected: &BTreeMap<u8, u64>) {
    let mut table_ids = runs
        .iter()
        .flat_map(|run| run.iter().map(|(id, _)| *id))
        .collect::<Vec<_>>();
    table_ids.sort_unstable();
    assert_eq!(table_ids, (0..tables.len()).collect::<Vec<_>>());

    for run in runs {
        for (index, (_, range)) in run.iter().enumerate() {
            for (_, other) in run.iter().skip(index + 1) {
                assert!(
                    !range.overlaps_with_key_range(other),
                    "optimized run contains overlapping tables: {run:?}"
                );
            }
        }
    }

    for (&key, &expected_seqno) in expected {
        let actual = runs.iter().find_map(|run| {
            run.iter()
                .find(|(_, range)| range.contains_key(&[key]))
                .and_then(|(id, _)| tables[*id].get(&key).copied())
        });

        assert_eq!(
            actual,
            Some(expected_seqno),
            "wrong visible version for key {key}: runs={runs:?}"
        );
    }
}

fn flush(
    buffer: &mut BTreeMap<u8, u64>,
    runs: &mut Runs,
    tables: &mut Vec<Table>,
    expected: &BTreeMap<u8, u64>,
) {
    if !buffer.is_empty() {
        let min = *buffer.first_key_value().expect("buffer is not empty").0;
        let max = *buffer.last_key_value().expect("buffer is not empty").0;
        let id = tables.len();

        tables.push(std::mem::take(buffer));
        runs.insert(
            0,
            vec![(id, KeyRange::new((vec![min].into(), vec![max].into())))],
        );
        *runs = optimize_runs(std::mem::take(runs));
    }

    verify(runs, tables, expected);
}

fn run_operations(operations: impl IntoIterator<Item = Operation>) {
    let mut buffer = BTreeMap::new();
    let mut expected = BTreeMap::new();
    let mut tables = Vec::new();
    let mut runs = Vec::new();

    for operation in operations.into_iter().take(256) {
        match operation {
            Operation::Insert { key, seqno } => {
                buffer.insert(key, seqno);
                expected.insert(key, seqno);
            }
            Operation::Flush => flush(&mut buffer, &mut runs, &mut tables, &expected),
        }
    }

    flush(&mut buffer, &mut runs, &mut tables, &expected);
}

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);
        let Ok(operations) = Vec::<Operation>::arbitrary(&mut unstructured) else {
            return;
        };

        run_operations(operations);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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
