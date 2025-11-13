use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn guard_into_inner_if() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;

        tree.insert("earth#name", "earth", 0);
        tree.insert("earth#color", "BLUE", 0);

        assert_eq!(2, tree.iter(SeqNo::MAX, None).count());

        assert_eq!(
            1,
            tree.iter(SeqNo::MAX, None)
                .filter_map(|guard| {
                    guard
                        .into_inner_if(|key| key.ends_with(b"#name"))
                        .unwrap()
                        .1
                })
                .count(),
        );
    }

    Ok(())
}
