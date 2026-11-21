// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::run::Ranged;
use crate::version::Run;

#[doc(hidden)]
#[must_use]
pub fn optimize_runs<T: Clone + Ranged>(runs: Vec<Run<T>>) -> Vec<Run<T>> {
    if runs.len() <= 1 {
        runs
    } else {
        let mut new_runs: Vec<Run<T>> = Vec::new();

        for run in &runs {
            for table in run.iter() {
                // NOTE: A table needs to end up behind every run that overlaps it,
                // otherwise point reads would find an older version of a key first
                let last_overlap = new_runs.iter().rposition(|existing_run| {
                    existing_run
                        .iter()
                        .any(|x| table.key_range().overlaps_with_key_range(x.key_range()))
                });

                let target = match last_overlap {
                    Some(idx) => new_runs.get_mut(idx + 1),
                    None => new_runs.first_mut(),
                };

                if let Some(target) = target {
                    target.push(table.clone());
                } else {
                    #[expect(
                        clippy::expect_used,
                        reason = "we pass in a table, so the run cannot be None"
                    )]
                    new_runs.push(Run::new(vec![table.clone()]).expect("run should not be empty"));
                }
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

    #[test]
    fn optimize_runs_overlap_transitive() {
        let runs = vec![
            Run::new(vec![s(2, "m", "p")]).unwrap(),
            Run::new(vec![s(1, "a", "z")]).unwrap(),
            Run::new(vec![s(0, "a", "c")]).unwrap(),
        ];
        let runs = optimize_runs::<FakeTable>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(2, "m", "p")]).unwrap(),
                Run::new(vec![s(1, "a", "z")]).unwrap(),
                Run::new(vec![s(0, "a", "c")]).unwrap(),
            ],
            &*runs
        );
    }
}
