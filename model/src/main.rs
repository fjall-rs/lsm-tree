use byteview::ByteView;
use clap::Parser;
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy},
    AbstractTree, Guard, KvSeparationOptions, SeqNo,
};
use rand::{seq::IteratorRandom, Rng};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap as ModelTree,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

/// Choose a key using a zipfian distribution biased towards the
/// end of the range.
/// The key is chosen from the range [0, written_count), except for
/// the case when written_count is 0, in which case 0 is returned.
pub fn choose_zipf(rng: &mut impl Rng, exponent: f64, written_count: u64) -> u64 {
    use rand::prelude::Distribution;
    use zipf::ZipfDistribution;

    if written_count == 0 {
        return 0;
    }

    written_count
        - ZipfDistribution::new(written_count as usize, exponent)
            .unwrap()
            .sample(rng) as u64
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Operation {
    PointRead(ByteView, SeqNo),
    Insert(ByteView, ByteView, SeqNo),
    Remove(ByteView, SeqNo),
    Flush(SeqNo),
    Compact(SeqNo),
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::PointRead(key, seqno) => {
                writeln!(f, "tree.get({:?}, {seqno});", key)
            }
            Operation::Insert(key, value, seqno) => {
                writeln!(f, "tree.insert({:?}, {:?}, {seqno});", key, value)
            }
            Operation::Remove(key, seqno) => {
                writeln!(f, "tree.remove({:?}, {seqno});", key)
            }
            Operation::Flush(seqno) => {
                writeln!(f, "tree.flush_active_memtable({seqno})?;")
            }
            Operation::Compact(seqno) => {
                writeln!(f, "tree.compact(compaction.clone(), {seqno})?;")
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

    #[clap(long, default_value_t = 100_000)]
    full_check_interval: usize,

    #[clap(long)]
    rerun: Option<PathBuf>,
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
            use std::io::Write;

            println!("Writing op log");

            let file = std::fs::File::create("oplog").unwrap();
            let mut file = std::io::BufWriter::new(file);

            let json_file = std::fs::File::create("oplog.jsonl").unwrap();
            let mut json_file = std::io::BufWriter::new(json_file);

            for op in &self.0 {
                // print!("{op}");
                write!(file, "{op}").unwrap();
                writeln!(json_file, "{}", serde_json::to_string(&op).unwrap()).unwrap();
            }

            file.flush().unwrap();
            file.get_mut().sync_all().unwrap();

            json_file.flush().unwrap();
            json_file.get_mut().sync_all().unwrap();

            eprintln!("-- Written oplog file to ./oplog --");
            eprintln!("-- Written oplog jsonl file to ./oplog.jsonl --");
        }
    }
}

fn run_oplog(oplog: &[Operation]) -> lsm_tree::Result<bool> {
    let folder = tempfile::tempdir_in("/king/tmp")?;

    let db = lsm_tree::Config::new(folder.path())
        .data_block_compression_policy(CompressionPolicy::disabled())
        .index_block_compression_policy(CompressionPolicy::disabled())
        .data_block_size_policy(BlockSizePolicy::all(100))
        // .index_block_size_policy(BlockSizePolicy::all(100))
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(10),
        ))
        .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Leveled {
        target_size: 32_000,
        ..Default::default()
    });

    let mut model = ModelTree::<ByteView, ByteView>::new();

    for op in oplog {
        match op {
            Operation::PointRead(key, s) => {
                let real_v = db.get(&key, *s)?;
                let model_v = model.get(&*key).cloned().map(lsm_tree::Slice::from);
                assert_eq!(model_v, real_v, "point read does not match");
            }
            Operation::Insert(key, value, s) => {
                db.insert(key.clone(), value.clone(), *s);
                model.insert(key.clone(), value.clone().into());
            }
            Operation::Remove(key, s) => {
                db.remove(key.clone(), *s);
                model.remove(key.into());
            }
            Operation::Flush(watermark) => {
                db.flush_active_memtable(*watermark)?;
            }
            Operation::Compact(watermark) => {
                db.compact(compaction.clone(), *watermark)?;
            }
            _ => unreachable!(),
        }
    }

    db.drop_range::<&[u8], _>(..)?;

    Ok(db.segment_count() == 0 && db.blob_file_count() == 0)
}

fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::from_default_env().init();

    let args = Args::parse();

    if let Some(oplog_path) = args.rerun {
        let file = File::open(oplog_path)?;
        let reader = BufReader::new(file);

        let mut oplog = vec![];

        for line in reader.lines() {
            let line = line?;
            let op: Operation = serde_json::from_str(&line).unwrap();
            oplog.push(op);
        }

        eprintln!("recovered oplog with {} operations", oplog.len());

        {
            if run_oplog(&oplog)? {
                eprintln!("initial op log is OK - exiting");
            } else {
                eprintln!("initial op log is failing - trying to minimize");
            }
        }

        let mut changed = false;

        loop {
            eprintln!("op log len: {}", oplog.len());

            for splice_idx in (0..oplog.len()).rev() {
                eprintln!("splice {splice_idx}");

                let mut copy = oplog.clone();
                copy.remove(splice_idx);

                if run_oplog(&copy)? {
                    // eprintln!("op log is OK - trying another index");
                } else {
                    eprintln!("op log is failing - trying to minimize further");
                    oplog = copy;
                    changed = true;
                }
            }

            if !changed {
                break;
            }

            changed = false;
        }

        {
            use std::io::Write;

            let file = std::fs::File::create("oplog_min").unwrap();
            let mut file = std::io::BufWriter::new(file);

            let json_file = std::fs::File::create("oplog_min.jsonl").unwrap();
            let mut json_file = std::io::BufWriter::new(json_file);

            for op in oplog {
                write!(file, "{op}").unwrap();
                writeln!(json_file, "{}", serde_json::to_string(&op).unwrap()).unwrap();
            }

            file.flush().unwrap();
            file.get_mut().sync_all().unwrap();

            json_file.flush().unwrap();
            json_file.get_mut().sync_all().unwrap();
        }

        return Ok(());
    }

    let mut rng = rand::thread_rng();
    let folder = tempfile::tempdir_in("/king/tmp")?;
    eprintln!("Using DB folder: {folder:?}");
    std::thread::sleep(std::time::Duration::from_millis(1_000));

    // TODO: append to log file instead?
    let mut op_log = OpLog::default();

    let mut model = ModelTree::<ByteView, ByteView>::new();

    let db = lsm_tree::Config::new(folder.path())
        .data_block_compression_policy(CompressionPolicy::disabled())
        .index_block_compression_policy(CompressionPolicy::disabled())
        .data_block_size_policy(BlockSizePolicy::all(100))
        // .index_block_size_policy(BlockSizePolicy::all(100))
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(10),
        ))
        .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Leveled {
        target_size: 32_000,
        ..Default::default()
    });
    let seqno = lsm_tree::SequenceNumberCounter::default();

    for x in 0u64..100_000 {
        let key: ByteView = x.to_be_bytes().into();
        let value: ByteView = ByteView::new(b"hellohello");
        let s = seqno.next();

        db.insert(key.clone(), value.clone(), s);
        model.insert(key.clone(), value.clone().into());

        op_log.push(Operation::Insert(key.clone(), value.clone(), s));
    }
    op_log.push(Operation::Flush(0));
    db.flush_active_memtable(0)?;

    for i in 0..args.ops.unwrap_or(usize::MAX) {
        let op = match (0usize..100).choose(&mut rng).unwrap() {
            0..50 => Operation::Insert(
                rng.gen_range(0u64..100_000).to_be_bytes().into(),
                ByteView::new(b"hellohello"),
                seqno.next(),
            ),
            // 50.. => Operation::Remove(
            //     rng.gen_range(0u64..100_000).to_be_bytes().to_vec(),
            //     seqno.next(),
            // ),
            _ => {
                let key = choose_zipf(&mut rng, 1.0, 100_000);
                let key = key.to_be_bytes().into();
                Operation::PointRead(key, seqno.get())
            }
        };

        if args.verbose || i % 100_000 == 0 {
            eprintln!("[{i}] Apply: {op:?}");
        }

        op_log.push(op.clone());

        match op {
            Operation::PointRead(key, s) => {
                let model_v = model.get(&key).cloned().map(lsm_tree::Slice::from);
                let real_v = db.get(&key, s)?;
                assert_eq!(model_v, real_v, "point read does not match");
            }
            Operation::Insert(key, value, s) => {
                db.insert(key.clone(), value.clone(), s);
                model.insert(key.clone(), value.clone().into());

                // let v = model.get(&key).unwrap();
                // assert_eq!(v, &value);

                // let v = db.get(&key, seqno.get())?.unwrap();
                // assert_eq!(
                //     v, &value,
                //     "value (of point read) for key {key:?} does not match",
                // );
            }
            Operation::Remove(key, s) => {
                db.remove(key.clone(), s);
                model.remove(&key);

                // assert!(
                //     !model.contains_key(&key),
                //     "model should not contain deleted key {key:?}",
                // );
                // assert!(
                //     !db.contains_key(&key, seqno.get())?,
                //     "db should not contain deleted key {key:?}",
                // );
            }
            _ => unreachable!(),
        }

        // Don't do so often because it's expensive
        if args.full_check_interval > 0 && i % args.full_check_interval == 0 {
            // log::trace!("  Full check");

            for (expected, guard) in model.iter().zip(db.iter(seqno.get(), None)) {
                let (k, v) = expected;
                let (real_k, real_v) = &guard.into_inner()?;
                let real_k = &**real_k;
                let real_v = &**real_v;

                assert_eq!(&**k, real_k, "key does not match");
                assert_eq!(&**v, real_v, "value for key {k:?} does not match");

                // Additionally, do a point read as well (because it's a different read path)
                let v = db.get(k, seqno.get())?.unwrap();
                assert_eq!(
                    v, real_v,
                    "value (of point read) for key {k:?} does not match",
                );
            }
        }

        if i % args.compaction_interval == 0 {
            if args.verbose {
                eprintln!("  Running flush + compaction");
            }

            let watermark = seqno.get().saturating_sub(100);
            op_log.push(Operation::Flush(watermark));
            db.flush_active_memtable(watermark)?;
            op_log.push(Operation::Compact(watermark));
            db.compact(compaction.clone(), watermark)?;
        }

        if op_log.len() > 10_000_000 || db.disk_space() > /* 1 GiB */ 1 * 1_024 * 1_024 * 1_024 {
            eprintln!(
                "DB size: {}, # tables: {}, # blob files: {}",
                db.disk_space(),
                db.segment_count(),
                db.blob_file_count(),
            );
            eprintln!("-- Clearing state --");
            std::thread::sleep(std::time::Duration::from_millis(500));

            op_log.push(Operation::Flush(seqno.get()));
            db.flush_active_memtable(seqno.get())?;
            db.drop_range::<&[u8], _>(..)?;
            model.clear();

            eprintln!(
                "DB size: {} MiB, # tables: {}, # blob files: {}",
                db.disk_space() / 1_024 / 1_024,
                db.segment_count(),
                db.blob_file_count(),
            );

            assert_eq!(0, db.segment_count());
            assert_eq!(0, db.blob_file_count());

            assert!(db.is_empty(seqno.get(), None)?);
            assert!(model.is_empty());

            eprintln!("-- Cleared state --");

            op_log.clear();

            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    Ok(())
}
