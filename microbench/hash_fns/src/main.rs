use hash_bench::*;
use rand::RngCore;
use std::{path::Path, time::Instant};

fn main() {
    let hashers: &[&dyn Hash64] = &[
        /* NOTE: GxHash needs AES instructions and a manual build flag, so a bit annoying to compile
        but it's very fast:
            RUSTFLAGS="-C target-cpu=native" cargo run -r
        */
        // &GxHash,
        &Xxh64,
        &Xxh3,
        //  &Xxh3_B, // NOTE: twox-hash is slower than xxhash-rust
        &RapidHash, &CityHash, &MetroHash,
        &WyHash,
        // &Fnv,
        // &RustcHash, // NOTE: rustc_hash is supposedly stable, but stability is a non-goal: https://github.com/rust-lang/rustc-hash/pull/56#issuecomment-2667670854
        // &FxHash, // NOTE:    ^ same for fxhash
        // &SeaHash, // NOTE: seahash is pretty slow
    ];

    let mut rng = rand::rng();

    let mut output = Vec::with_capacity(hashers.len());

    for hasher in hashers {
        for (byte_len, invocations) in [
            (4, 1_000_000_000),
            (8, 1_000_000_000),
            (16, 1_000_000_000),
            (32, 1_000_000_000),
            (64, 1_000_000_000),
            (128, 500_000_000),
            (256, 250_000_000),
            (512, 125_000_000),
            (1_024, 64_000_000),
            (4 * 1_024, 16_000_000),
            (8 * 1_024, 8_000_000),
            (16 * 1_024, 4_000_000),
            (32 * 1_024, 2_000_000),
            (64 * 1_024, 1_000_000),
        ] {
            let invocations = if hasher.name() == "FNV" {
                invocations / 4 / 10
            } else {
                invocations / 4
            };

            let mut bytes = vec![0; byte_len];
            rng.fill_bytes(&mut bytes);
            eprint!("{} ({} bytes): ", hasher.name(), bytes.len());

            let start = Instant::now();
            for _ in 0..invocations {
                std::hint::black_box(hasher.hash64(&bytes));
            }
            let elapsed = start.elapsed();
            let ns = elapsed.as_nanos();
            let per_call = ns as f64 / invocations as f64;

            eprintln!("{elapsed:?} - {per_call}ns per invocation");

            output.push(format!(
                "{{\"hash\": {:?},\"byte_len\": {byte_len}, \"ns\": {per_call}}}",
                hasher.name(),
            ));
        }

        eprintln!();
    }

    {
        let output = Path::new("hash.png");

        if output.exists() {
            std::fs::remove_file(&output).unwrap();
        }
    }

    {
        let data = output.join("\n");

        let template = std::fs::read_to_string("template.py").unwrap();
        let template = template.replace("% data %", &data);
        std::fs::write("tmp.py", &template).unwrap();
        std::fs::write("data.jsonl", &data).unwrap();
    }

    {
        let status = std::process::Command::new("python3")
            .arg("tmp.py")
            .status()
            .unwrap();

        std::fs::remove_file("tmp.py").unwrap();

        assert!(status.success(), "python failed");
    }

    // TODO: bench conflicts 1B keys - if >0, create matplotlib python image as well, also save JSON
}
