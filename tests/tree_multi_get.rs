use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_multi_get_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 1);
    tree.insert("c", "c", 2);

    // Test getting existing keys
    let keys = vec!["a".as_bytes(), "c".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some("a".as_bytes()));
    assert_eq!(values[1].as_deref(), Some("c".as_bytes()));

    // Test getting a mix of existing and non-existing keys
    let keys = vec!["a".as_bytes(), "d".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some("a".as_bytes()));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref(), Some("b".as_bytes()));

    // Test getting non-existing key
    let keys = vec!["d".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 1);
    assert_eq!(values[0], None);

    // Test with flush
    tree.flush_active_memtable(2)?;

    let keys = vec!["a".as_bytes(), "d".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some("a".as_bytes()));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref(), Some("b".as_bytes()));

    Ok(())
}

#[test]
fn tree_multi_get_overwrite() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a_old", 0);
    tree.insert("b", "b", 1);
    tree.insert("a", "a_new", 2);
    tree.insert("c", "c", 3);

    // Test getting overwriten keys
    let keys = vec!["a".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some("a_new".as_bytes()));
    assert_eq!(values[1].as_deref(), Some("b".as_bytes()));

    // Test with flush
    tree.flush_active_memtable(3)?;

    let keys = vec!["a".as_bytes(), "b".as_bytes()];
    let values = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some("a_new".as_bytes()));
    assert_eq!(values[1].as_deref(), Some("b".as_bytes()));

    Ok(())
}

#[test]
fn tree_multi_get_consistency() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 1);

    // Compare with get
    let multi_get_val = tree.multi_get(&["a".as_bytes()], SeqNo::MAX)?;
    let get_val = tree.get("a", SeqNo::MAX)?;

    assert_eq!(multi_get_val.len(), 1);
    assert_eq!(multi_get_val[0], get_val);

    // Compare with get on non-existing key
    let multi_get_val = tree.multi_get(&["c".as_bytes()], SeqNo::MAX)?;
    let get_val = tree.get("c", SeqNo::MAX)?;

    assert_eq!(multi_get_val.len(), 1);
    assert_eq!(multi_get_val[0], get_val);

    Ok(())
}

mod sudo_required {
    use lsm_tree::config::{CompressionPolicy, FilterPolicy, FilterPolicyEntry};
    use lsm_tree::{AbstractTree, CompressionType, Config, SeqNo, SequenceNumberCounter};
    use std::hint::black_box;
    use std::process::Command;
    use std::time::Instant;

    fn drop_caches() {
        let _ = Command::new("sudo")
            .arg("sh")
            .arg("-c")
            .arg("echo 3 > /proc/sys/vm/drop_caches")
            .status();
    }

    #[test]
    #[ignore = "This is a machine-dependent benchmark that requires root privileges for dropping caches. Run manually with sudo cargo test -- --ignored."]
    fn multi_get_scattered_large_values_outperforms_single_gets() -> lsm_tree::Result<()> {
        let folder = tempfile::tempdir()?;

        let mut config = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        );

        // Disable compression to ensure large I/O
        config =
            config.data_block_compression_policy(CompressionPolicy::all(CompressionType::None));

        // Disable block cache
        config = config.use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(0)));

        // Disable filters to reduce CPU overhead
        config = config.filter_policy(FilterPolicy::all(FilterPolicyEntry::None));

        let tree = config.open()?;

        let num_ssts = 64;
        let keys_per_sst = 1; // One key per SST for maximum scatter
        let value_size = 1024 * 1024; // 1 MiB to force significant I/O

        let mut all_keys = Vec::new();
        let mut seq = 0u64;

        for i in 0..num_ssts {
            for k in 0..keys_per_sst {
                let key = format!("scatter-key-{}-{}", i, k).into_bytes();
                let value = vec![0xab; value_size];

                tree.insert(key.clone(), value, seq);
                all_keys.push(key);
                seq += 1;
            }

            // Force flush to create a new SST for each group (here, per key)
            tree.flush_active_memtable(seq)?;
        }

        // Verify we have the expected number of tables/SSTs
        let table_count = tree.table_count();
        assert_eq!(
            table_count, num_ssts as usize,
            "Expected one table per flush"
        );

        // Warm-up: Load metadata without caching data blocks (cache is disabled anyway)
        for key in &all_keys {
            let _ = tree.get(&key[..], SeqNo::MAX)?;
        }

        let num_runs = 10;

        // Measure sequential gets over multiple runs
        let mut single_get_times_ns: Vec<u128> = Vec::with_capacity(num_runs);
        for _ in 0..num_runs {
            drop_caches();

            let start = Instant::now();
            for key in &all_keys {
                let res = tree.get(&key[..], SeqNo::MAX)?;
                black_box(res);
            }
            let elapsed = start.elapsed();
            single_get_times_ns.push(elapsed.as_nanos());
        }

        let avg_single_ns: f64 = single_get_times_ns.iter().sum::<u128>() as f64 / num_runs as f64;
        println!(
            "Average sequential gets time: {:.2} ms",
            avg_single_ns / 1_000_000.0
        );

        // Measure multi_get over multiple runs
        let mut multi_get_times_ns: Vec<u128> = Vec::with_capacity(num_runs);
        let key_slices: Vec<&[u8]> = all_keys.iter().map(|k| k.as_slice()).collect();

        for _ in 0..num_runs {
            drop_caches();

            let start = Instant::now();
            let res = tree.multi_get(&key_slices, SeqNo::MAX)?;
            black_box(&res);
            let elapsed = start.elapsed();
            multi_get_times_ns.push(elapsed.as_nanos());
        }

        let avg_multi_ns: f64 = multi_get_times_ns.iter().sum::<u128>() as f64 / num_runs as f64;
        println!(
            "Average multi_get time: {:.2} ms",
            avg_multi_ns / 1_000_000.0
        );

        // Assert significant speedup (expect multi_get to be at least 2x faster, but likely more on real hardware)
        assert!(
            avg_multi_ns * 2.0 < avg_single_ns,
            "MultiGet did not significantly outperform sequential gets (avg {} ms vs {} ms)",
            avg_multi_ns / 1_000_000.0,
            avg_single_ns / 1_000_000.0
        );

        Ok(())
    }
}
