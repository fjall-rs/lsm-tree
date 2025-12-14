// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::run::Ranged;
use crate::version::Run;

pub fn optimize_runs<T: Clone + Ranged>(runs: Vec<Run<T>>) -> Vec<Run<T>> {
    if runs.len() <= 1 {
        runs
    } else {
        let mut new_runs: Vec<Run<T>> = Vec::new();

        for run in runs.iter().rev() {
            'run: for table in run.iter().rev() {
                for existing_run in new_runs.iter_mut().rev() {
                    if existing_run
                        .iter()
                        .all(|x| !table.key_range().overlaps_with_key_range(x.key_range()))
                    {
                        existing_run.push(table.clone());
                        continue 'run;
                    }
                }

                #[expect(
                    clippy::expect_used,
                    reason = "we pass in a table, so the run cannot be None"
                )]
                new_runs.insert(
                    0,
                    Run::new(vec![table.clone()]).expect("run should not be empty"),
                );
            }
        }

        new_runs
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::KeyRange;
    use test_log::test;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeTable {
        id: u64,
        key_range: KeyRange,
    }

    impl Ranged for FakeTable {
        fn key_range(&self) -> &KeyRange {
            &self.key_range
        }
    }

    fn s(id: u64, min: &str, max: &str) -> FakeTable {
        FakeTable {
            id,
            key_range: KeyRange::new((min.as_bytes().into(), max.as_bytes().into())),
        }
    }

    #[test]
    fn optimize_runs_empty() {
        let runs = vec![];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(Vec::<Run<FakeTable>>::new(), &*runs);
    }

    #[test]
    fn optimize_runs_one() {
        let runs = vec![Run::new(vec![s(0, "a", "b")]).unwrap()];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(vec![Run::new(vec![s(0, "a", "b")]).unwrap()], &*runs);
    }

    #[test]
    fn optimize_runs_two_overlap() {
        let runs = vec![
            Run::new(vec![s(0, "a", "b")]).unwrap(),
            Run::new(vec![s(1, "a", "b")]).unwrap(),
        ];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(0, "a", "b")]).unwrap(),
                Run::new(vec![s(1, "a", "b")]).unwrap(),
            ],
            &*runs
        );
    }

    #[test]
    fn optimize_runs_two_overlap_2() {
        let runs = vec![
            Run::new(vec![s(0, "a", "z")]).unwrap(),
            Run::new(vec![s(1, "c", "f")]).unwrap(),
        ];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(0, "a", "z")]).unwrap(),
                Run::new(vec![s(1, "c", "f")]).unwrap(),
            ],
            &*runs
        );
    }

    #[test]
    fn optimize_runs_two_overlap_3() {
        let runs = vec![
            Run::new(vec![s(0, "c", "f")]).unwrap(),
            Run::new(vec![s(1, "a", "z")]).unwrap(),
        ];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(0, "c", "f")]).unwrap(),
                Run::new(vec![s(1, "a", "z")]).unwrap()
            ],
            &*runs
        );
    }

    #[test]
    fn optimize_runs_two_disjoint() {
        let runs = vec![
            Run::new(vec![s(0, "a", "c")]).unwrap(),
            Run::new(vec![s(1, "d", "f")]).unwrap(),
        ];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(
            vec![Run::new(vec![s(0, "a", "c"), s(1, "d", "f")]).unwrap()],
            &*runs,
        );
    }

    #[test]
    fn optimize_runs_two_disjoint_2() {
        let runs = vec![
            Run::new(vec![s(1, "d", "f")]).unwrap(),
            Run::new(vec![s(0, "a", "c")]).unwrap(),
        ];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(
            vec![Run::new(vec![s(0, "a", "c"), s(1, "d", "f")]).unwrap()],
            &*runs,
        );
    }
}
