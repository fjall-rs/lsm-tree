use clap::Parser;
use lsm_tree::{
    AbstractTree, Guard, UserKey as Key, UserValue as Value,
    config::{BlockSizePolicy, CompressionPolicy},
};
use rand::{Rng, seq::IteratorRandom};
use std::{collections::BTreeMap as ModelTree, sync::Arc};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Debug)]
enum Operation {
    Insert(Key, Value),
    Remove(Key),
}

// clap derive
#[derive(Parser)]
struct Args {
    #[clap(long)]
    ops: Option<usize>,

    #[clap(long, default_value_t = 250)]
    compaction_every: usize,

    #[clap(long, default_value_t = false)]
    verbose: bool,
}

fn main() -> lsm_tree::Result<()> {
    let args = Args::parse();

    let mut rng = rand::rng();
    let folder = tempfile::tempdir()?;
    eprintln!("Using DB folder: {folder:?}");
    std::thread::sleep(std::time::Duration::from_millis(1_000));

    // TODO: append to log file instead
    let mut op_log = vec![];

    let mut model = ModelTree::new();
    let db = lsm_tree::Config::new(folder.path())
        .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::Lz4))
        .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::Lz4))
        .data_block_size_policy(BlockSizePolicy::all(100))
        .index_block_size_policy(BlockSizePolicy::all(100))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());
    let seqno = lsm_tree::SequenceNumberCounter::default();

    for i in 0..args.ops.unwrap_or(usize::MAX) {
        let op = match (0usize..100).choose(&mut rng).unwrap() {
            0..50 => Operation::Insert(Key::from(seqno.get().to_be_bytes()), Value::from("hello")),
            50.. => Operation::Remove(Key::from(rng.random::<u64>().to_be_bytes())),
        };

        if args.verbose || i % 100 == 0 {
            eprintln!("[{i}] Apply: {op:?}");
        }

        op_log.push(op.clone());

        match op {
            Operation::Insert(key, value) => {
                db.insert(key.clone(), value.clone(), seqno.next());
                model.insert(key.clone(), value.clone());

                let v = model.get(&key).unwrap();
                assert_eq!(v, &value);

                let v = db.get(&key, seqno.get())?.unwrap();
                assert_eq!(
                    v, &value,
                    "value (of point read) for key {key:?} does not match",
                );
            }
            Operation::Remove(key) => {
                db.remove(key.clone(), seqno.next());
                model.remove(&key);

                assert!(
                    !model.contains_key(&key),
                    "model should not contain deleted key {key:?}",
                );
                assert!(
                    !db.contains_key(&key, seqno.get())?,
                    "db should not contain deleted key {key:?}",
                );
            }
        }

        // Don't do so often because it's expensive
        if i % 10_000 == 0 {
            eprintln!("  Full check");

            for (expected, guard) in model.iter().zip(db.iter(seqno.get(), None)) {
                let (k, v) = expected;
                let (real_k, real_v) = &guard.into_inner()?;
                assert_eq!(k, real_k, "key does not match");
                assert_eq!(v, real_v, "value for key {k:?} does not match");

                // Additionally, do a point read as well (because it's a different read path)
                let v = db.get(k, seqno.get())?.unwrap();
                assert_eq!(
                    v, real_v,
                    "value (of point read) for key {k:?} does not match",
                );
            }
        }

        if i % args.compaction_every == 0 {
            eprintln!("  Running flush + compaction");
            let watermark = seqno.get().saturating_sub(100);
            db.flush_active_memtable(watermark)?;
            db.compact(compaction.clone(), watermark)?;
        }

        if op_log.len() > 500_000 || db.disk_space() > /* 1 GiB */ 1 * 1_024 * 1_024 * 1_024 {
            eprintln!("-- Clearing state --");
            eprintln!(
                "DB size: {}, # tables: {}",
                db.disk_space(),
                db.segment_count(),
            );
            std::thread::sleep(std::time::Duration::from_millis(500));

            db.drop_range::<&[u8], _>(..)?;
            model.clear();
            op_log.clear();

            assert!(db.is_empty(seqno.get(), None)?);
            assert!(model.is_empty());

            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    Ok(())
}
