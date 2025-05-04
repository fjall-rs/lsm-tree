use lsm_tree::{
    segment::filter::{
        blocked_bloom::Builder as BlockedBloomBuilder,
        standard_bloom::Builder as StandardBloomBuilder, AMQ,
    },
    Result,
};

// [Theoretical] FPR: 1.0000%, [Empirical] Standard Bloom FPR: 0.0002, Blocked Bloom FPR: 0.0313%
// [Theoretical] FPR: 0.1000%, [Empirical] Standard Bloom FPR: 0.0000, Blocked Bloom FPR: 0.0303%
// [Theoretical] FPR: 0.0100%, [Empirical] Standard Bloom FPR: 0.0000, Blocked Bloom FPR: 0.0287%
// [Theoretical] FPR: 0.0010%, [Empirical] Standard Bloom FPR: 0.0000, Blocked Bloom FPR: 0.0257%
#[test]
fn measure_bloom_fpr_with_fp_rate() -> Result<()> {
    let keys = (0..1_000_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    let non_existent_keys = (1_000_000..2_000_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    let n: usize = 5_000_000;

    for fpr in [0.01, 0.001, 0.0001, 0.00001] {
        let mut blocked_builder = BlockedBloomBuilder::with_fp_rate(n, fpr);
        let mut standard_builder = StandardBloomBuilder::with_fp_rate(n, fpr);

        for key in &keys {
            blocked_builder.set_with_hash(BlockedBloomBuilder::get_hash(key.as_slice()));
            standard_builder.set_with_hash(StandardBloomBuilder::get_hash(key.as_slice()));
        }

        let blocked_filter = blocked_builder.build();
        let standard_filter = standard_builder.build();

        let mut blocked_fp = 0;
        let mut standard_fp = 0;
        for non_existent_key in &non_existent_keys {
            if blocked_filter
                .contains_hash(BlockedBloomBuilder::get_hash(non_existent_key.as_slice()))
            {
                blocked_fp += 1;
            }
            if standard_filter
                .contains_hash(StandardBloomBuilder::get_hash(non_existent_key.as_slice()))
            {
                standard_fp += 1;
            }
        }

        println!(
            "[Theoretical] FPR: {:.4}%, [Empirical] Standard Bloom FPR: {:.4}, Blocked Bloom FPR: {:.4}%",
            fpr * 100.0,
            (standard_fp as f64 / non_existent_keys.len() as f64) * 100.0,
            (blocked_fp as f64 / non_existent_keys.len() as f64) * 100.0
        );
    }

    Ok(())
}

// n = 5000000, [Empirical] Standard Bloom FPR: 0.0006, Blocked Bloom FPR: 0.0276%
// n = 10000000, [Empirical] Standard Bloom FPR: 0.0000, Blocked Bloom FPR: 0.0108%
// n = 15000000, [Empirical] Standard Bloom FPR: 0.0000, Blocked Bloom FPR: 0.0086%
#[test]
fn measure_bloom_fpr_with_bpk() -> Result<()> {
    let keys = (0..1_000_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    let non_existent_keys = (1_000_000..2_000_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    for n in [5_000_000, 10_000_000, 15_000_000] {
        let mut blocked_builder = BlockedBloomBuilder::with_bpk(n, 10);
        let mut standard_builder = StandardBloomBuilder::with_bpk(n, 10);

        for key in &keys {
            blocked_builder.set_with_hash(BlockedBloomBuilder::get_hash(key.as_slice()));
            standard_builder.set_with_hash(StandardBloomBuilder::get_hash(key.as_slice()));
        }

        let blocked_filter = blocked_builder.build();
        let standard_filter = standard_builder.build();

        let mut blocked_fp = 0;
        let mut standard_fp = 0;
        for non_existent_key in &non_existent_keys {
            if blocked_filter
                .contains_hash(BlockedBloomBuilder::get_hash(non_existent_key.as_slice()))
            {
                blocked_fp += 1;
            }
            if standard_filter
                .contains_hash(StandardBloomBuilder::get_hash(non_existent_key.as_slice()))
            {
                standard_fp += 1;
            }
        }

        println!(
            "n = {}, [Empirical] Standard Bloom FPR: {:.4}, Blocked Bloom FPR: {:.4}%",
            n,
            (standard_fp as f64 / non_existent_keys.len() as f64) * 100.0,
            (blocked_fp as f64 / non_existent_keys.len() as f64) * 100.0
        );
    }

    Ok(())
}
