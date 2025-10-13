use clap::Parser;
use lsm_tree::{
    AbstractTree, Guard, KvSeparationOptions, SeqNo,
    config::{BlockSizePolicy, CompressionPolicy},
};
use rand::{Rng, seq::IteratorRandom};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap as ModelTree, sync::Arc};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Operation {
    Insert(Vec<u8>, Vec<u8>, SeqNo),
    Remove(Vec<u8>, SeqNo),
    FlushAndCompact(SeqNo),
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Insert(key, value, seqno) => {
                writeln!(f, "tree.insert({:?}, {:?}, {seqno});", key, value)
            }
            Operation::Remove(key, seqno) => {
                writeln!(f, "tree.remove({:?}, {seqno});", key)
            }
            Operation::FlushAndCompact(seqno) => {
                writeln!(f, "tree.flush_active_memtable({seqno})?;")?;
                writeln!(f, "tree.compact(compaction, {seqno})?;")
            }
        }
    }
}

// clap derive
#[derive(Parser)]
struct Args {
    #[clap(long)]
    ops: Option<usize>,

    #[clap(long, default_value_t = 250)]
    compaction_interval: usize,

    #[clap(long, default_value_t = false)]
    verbose: bool,

    #[clap(long, default_value_t = 10_000)]
    full_check_interval: usize,
}

#[derive(Default)]
struct OpLog(Vec<Operation>);

impl OpLog {
    fn push(&mut self, op: Operation) {
        self.0.push(op);
    }

    fn len(&mut self) -> usize {
        self.0.len()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

impl Drop for OpLog {
    fn drop(&mut self) {
        if std::thread::panicking() {
            println!("-- OP LOG --");

            for op in &self.0 {
                print!("{op}");
            }
        }
    }
}

fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::from_default_env().init();

    let args = Args::parse();

    let mut rng = rand::rng();
    let folder = tempfile::tempdir()?;
    eprintln!("Using DB folder: {folder:?}");
    std::thread::sleep(std::time::Duration::from_millis(1_000));

    // TODO: append to log file instead?
    let mut op_log = OpLog::default();

    let mut model = ModelTree::new();
    let db = lsm_tree::Config::new(folder.path())
        .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::Lz4))
        .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::Lz4))
        .data_block_size_policy(BlockSizePolicy::all(100))
        // .index_block_size_policy(BlockSizePolicy::all(100))
        // .with_kv_separation(Some(
        //     KvSeparationOptions::default().separation_threshold(10),
        // ))
        .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());
    let seqno = lsm_tree::SequenceNumberCounter::default();

    for i in 0..args.ops.unwrap_or(usize::MAX) {
        let op = match (0usize..100).choose(&mut rng).unwrap() {
            0..50 => Operation::Insert(
                rng.random_range(0u64..10).to_be_bytes().to_vec(),
                b"hellohello".to_vec(),
                seqno.next(),
            ),
            50.. => Operation::Remove(
                rng.random_range(0u64..10).to_be_bytes().to_vec(),
                seqno.next(),
            ),
        };

        if args.verbose || i % 100 == 0 {
            eprintln!("[{i}] Apply: {op:?}");
        }

        op_log.push(op.clone());

        match op {
            Operation::Insert(key, value, s) => {
                db.insert(key.clone(), value.clone(), s);
                model.insert(key.clone(), value.clone());

                let v = model.get(&key).unwrap();
                assert_eq!(v, &value);

                let v = db.get(&key, seqno.get())?.unwrap();
                assert_eq!(
                    v, &value,
                    "value (of point read) for key {key:?} does not match",
                );
            }
            Operation::Remove(key, s) => {
                db.remove(key.clone(), s);
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
            _ => unreachable!(),
        }

        // Don't do so often because it's expensive
        if i % args.full_check_interval == 0 {
            // log::trace!("  Full check");

            for (expected, guard) in model.iter().zip(db.iter(seqno.get(), None)) {
                let (k, v) = expected;
                let (real_k, real_v) = &guard.into_inner()?;
                assert_eq!(k, &**real_k, "key does not match");
                assert_eq!(v, &**real_v, "value for key {k:?} does not match");

                // Additionally, do a point read as well (because it's a different read path)
                let v = db.get(k, seqno.get())?.unwrap();
                assert_eq!(
                    v, real_v,
                    "value (of point read) for key {k:?} does not match",
                );
            }
        }

        if i % args.compaction_interval == 0 {
            eprintln!("  Running flush + compaction");
            let watermark = seqno.get().saturating_sub(100);
            op_log.push(Operation::FlushAndCompact(watermark));
            db.flush_active_memtable(watermark)?;
            db.compact(compaction.clone(), watermark)?;
        }

        if op_log.len() > 500_000 || db.disk_space() > /* 1 GiB */ 1 * 1_024 * 1_024 * 1_024 {
            eprintln!(
                "DB size: {}, # tables: {}, # blob files: {}",
                db.disk_space(),
                db.segment_count(),
                db.blob_file_count(),
            );
            eprintln!("-- Clearing state --");
            std::thread::sleep(std::time::Duration::from_millis(500));

            db.drop_range::<&[u8], _>(..)?;
            model.clear();
            op_log.clear();

            assert!(db.is_empty(seqno.get(), None)?);
            assert!(model.is_empty());

            eprintln!(
                "DB size: {}, # tables: {}, # blob files: {}",
                db.disk_space(),
                db.segment_count(),
                db.blob_file_count(),
            );
            eprintln!("-- Cleared state --");

            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    Ok(())
}
