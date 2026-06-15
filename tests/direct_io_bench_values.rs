// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[path = "../examples/direct_io_bench.rs"]
#[allow(dead_code)]
mod direct_io_bench;

use std::collections::HashSet;

#[test]
fn direct_io_bench_payload_filler_is_deterministic_and_seeded() {
    let mut first = vec![0; 4_096];
    let mut first_again = vec![0; 4_096];
    let mut second = vec![0; 4_096];

    direct_io_bench::fill_incompressible_value(&mut first, 1);
    direct_io_bench::fill_incompressible_value(&mut first_again, 1);
    direct_io_bench::fill_incompressible_value(&mut second, 2);

    assert_eq!(first, first_again);
    assert_ne!(first, second);

    let unique_chunks = first
        .chunks(8)
        .take(64)
        .map(<[u8]>::to_vec)
        .collect::<HashSet<_>>();
    assert!(
        unique_chunks.len() > 48,
        "payload should vary across chunks instead of repeating one 8-byte pattern",
    );
}
