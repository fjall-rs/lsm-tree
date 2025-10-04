use clap::Parser;
use lsm_tree::{AbstractTree, Guard, UserKey as Key, UserValue as Value, config::BlockSizePolicy};
use rand::seq::IteratorRandom;
use std::{collections::BTreeMap as ModelTree, sync::Arc};

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
        .data_block_size_policy(BlockSizePolicy::all(100))
        .index_block_size_policy(BlockSizePolicy::all(100))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());
    let seqno = lsm_tree::SequenceNumberCounter::default();

    for i in 0..args.ops.unwrap_or(usize::MAX) {
        let op = match (0..2).choose(&mut rng).unwrap() {
            0 => Operation::Insert(Key::from(seqno.get().to_be_bytes()), Value::from("hello")),
            1 => Operation::Remove(Key::from("b")),
            _ => unreachable!(),
        };

        if args.verbose || i % 100 == 0 {
            eprintln!("[{i}] Apply: {op:?}");
        }

        op_log.push(op.clone());

        match op {
            Operation::Insert(key, value) => {
                db.insert(key.clone(), value.clone(), seqno.next());
                model.insert(key, value);
            }
            Operation::Remove(key) => {
                db.remove(key.clone(), seqno.next());
                model.remove(&key);
            }
        }

        for (expected, guard) in model.iter().zip(db.iter(seqno.get(), None)) {
            let (k, v) = expected;
            let (real_k, real_v) = &guard.into_inner()?;
            assert_eq!(k, real_k, "key does not match");
            assert_eq!(v, real_v, "value for key {k:?} does not match");

            // Don't do so often because it's expensive
            if i % 100 == 0 {
                // Additionally, do a point read as well (because it's a different read path)
                let v = db.get(k, seqno.get())?.unwrap();
                assert_eq!(
                    v, real_v,
                    "value (of point read) for key {k:?} does not match",
                );
            }

            // TODO: also, we need to do range reads/prefix reads, can't possibly test every permutation though...
        }

        if i % args.compaction_every == 0 {
            eprintln!("  Running flush + compaction");
            let watermark = seqno.get().saturating_sub(100);
            db.flush_active_memtable(watermark)?;
            db.compact(compaction.clone(), watermark)?;
        }

        if op_log.len() > 10_000_000 || db.disk_space() > /* 1 GiB */ 1 * 1_024 * 1_024 * 1_024 {
            eprintln!("-- Clearing state --");
            std::thread::sleep(std::time::Duration::from_millis(100));
            db.drop_range::<&[u8], _>(..)?;
            model.clear();
            op_log.clear();
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    Ok(())
}
