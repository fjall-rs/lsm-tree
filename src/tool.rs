// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! CLI tool for interacting with LSM trees

use clap::{ArgAction, CommandFactory, Parser, Subcommand};
use humansize::{SizeFormatter, BINARY};
use lsm_tree::config::KvSeparationOptions;
use lsm_tree::{AbstractTree, AnyTree, Config, Guard, SeqNo, SequenceNumberCounter, ValueType};
use rustyline::DefaultEditor;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    prelude::*,
    registry::Registry,
};

macro_rules! die {
    ($fmt:literal, $($arg:tt)*) => {{
        eprintln!($fmt, $($arg)*);
        std::process::exit(1);
    }};

    ($msg:literal) => {{
        eprintln!($msg);
        std::process::exit(1);
    }};

    () => {{
        eprintln!("Program terminated unexpectedly");
        std::process::exit(1);
    }};
}

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub fn init_tracing(quiet: bool, verbose: u8) -> (bool, LevelFilter) {
    let is_verbose = !quiet && verbose > 0;

    let level_filter = if quiet {
        LevelFilter::ERROR
    } else {
        match verbose {
            0 => LevelFilter::WARN,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        }
    };

    // Bridge log crate macros to tracing (for library code that uses log::*)
    tracing_log::LogTracer::init().expect("Failed to set log tracer");

    let registry = Registry::default();

    let env_filter = EnvFilter::builder()
        .with_default_directive(level_filter.into())
        .with_env_var("LSM_LOG")
        .from_env_lossy()
        .add_directive(
            "rustyline=warn"
                .parse()
                .expect("Failed to parse rustyline directive"),
        );

    let subscriber = registry.with(env_filter).with(
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .compact(),
    );

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        die!("INTERNAL ERROR: setting default tracing::subscriber failed");
    }

    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing_panic::panic_hook(info);
        prev_hook(info); // daisy-chain to old panic hook
    }));

    (is_verbose, level_filter)
}

fn parse_size_as_u32(s: &str) -> Result<u32, String> {
    let cfg = parse_size::Config::new().with_binary();
    cfg.parse_size(s)
        .map(|size| size as u32)
        .map_err(|e| e.to_string())
}

static DEFAULT_KV_SEPARATION_OPTIONS: LazyLock<KvSeparationOptions> =
    LazyLock::new(|| KvSeparationOptions::default());
static DEFAULT_SEPARATION_THRESHOLD: LazyLock<String> = LazyLock::new(|| {
    SizeFormatter::new(DEFAULT_KV_SEPARATION_OPTIONS.separation_threshold, BINARY).to_string()
});

/// CLI tool for interacting with LSM trees
#[derive(Parser, Debug)]
#[command(name = "lsm")]
#[command(about = "CLI tool for interacting with LSM trees")]
struct ToolArgs {
    /// Suppress all output except for errors. This overrides the -v flag.
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Turn on verbose output. Supply -v multiple times to increase verbosity.
    #[arg(short, long, action = ArgAction::Count, global = true)]
    verbose: u8,

    /// Path to the LSM tree directory (will be created if it doesn't exist)
    lsm_path: PathBuf,

    /// Key/Value Separation Threshold (e.g., "1KiB", "4096")
    #[arg(
        short = 't', long,
        default_value = &**DEFAULT_SEPARATION_THRESHOLD,
        value_parser = parse_size_as_u32,
        value_name = "THRESHOLD",
    )]
    separation_threshold: u32,

    /// Open as Blob Tree
    #[arg(short, long, default_value_t = false)]
    blob_tree: bool,

    /// Command to run (if omitted, starts interactive shell)
    #[command(subcommand)]
    command: Option<ToolCommand>,
}

#[derive(Subcommand, Debug, Clone)]
enum ToolCommand {
    /// Get the value for a key
    Get {
        /// The key to look up
        key: String,
    },
    /// Set a key-value pair
    Set {
        /// The key to set
        key: String,
        /// The value to store
        value: String,
    },
    /// Delete a key
    Del {
        /// The key to delete
        key: String,
        /// Use weak tombstone instead of regular tombstone
        #[arg(short = 'w', long = "weak")]
        weak: bool,
    },
    /// List all keys, optionally filtered by prefix
    #[command(visible_alias = "list", visible_alias = "ls")]
    Scan {
        /// Optional prefix to filter keys
        prefix: Option<String>,

        /// Show internal key fields (seqno, value_type)
        #[arg(short = 'l', long = "long")]
        long: bool,
    },
    #[command(hide = true, alias = "ll")]
    ScanLong {
        /// Optional prefix to filter keys
        prefix: Option<String>,
    },
    /// List keys in a range [start, end)
    Range {
        /// Start of the range (inclusive)
        start: String,
        /// End of the range (exclusive)
        end: String,
    },
    /// Count the number of items
    Count,
    /// Flush memtable to disk
    Flush,
    /// Run major compaction
    Compact,
    /// Show tree statistics
    Info,
}

// Internal shell commands, include all external tool commands
#[derive(Parser, Debug)]
#[command(name = "")]
#[command(no_binary_name = true)]
#[command(disable_version_flag = true)]
#[command(help_template = "
{version}

Available Commands:

{subcommands}

Use `help COMMAND` or `COMMAND --help` for more details.

")]

struct ShellArgs {
    #[command(subcommand)]
    command: ShellCommand,
}

// Shell commands (including ones not available from CLI)
#[derive(Subcommand, Debug, Clone)]
enum ShellCommand {
    #[command(flatten)]
    ToolCmd(ToolCommand),

    /// Exit the current shell (with implicit flush)
    #[command(visible_alias = "quit")]
    Exit,
    /// Abort the curent shell (without flush)
    Abort,
    /// Begin a new batch (transaction)
    Begin,
    /// Commit the current batch
    Commit,
    /// Rollback (discard) the current batch
    Rollback,
}

/// A pending operation in a batch
#[derive(Debug, Clone)]
enum BatchOp {
    Set { key: String, value: String },
    Del { key: String },
}

/// A batch of pending operations
#[derive(Debug, Default)]
struct Batch {
    /// Operations in order they were added
    ops: Vec<BatchOp>,
    /// Current state of keys in the batch (for reads)
    state: HashMap<String, Option<String>>,
}

struct Session {
    tree: AnyTree,
    seqno: AtomicU64,
    path: PathBuf,
    batch: RefCell<Option<Batch>>,
}

impl Session {
    fn open(path: PathBuf, separation_threshold: u32, blob_tree: bool) -> lsm_tree::Result<Self> {
        let config = Config::new(
            &path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        );

        let config = if blob_tree {
            let kv_opts = KvSeparationOptions::default().separation_threshold(separation_threshold);
            config.with_kv_separation(Some(kv_opts))
        } else {
            config
        };

        let tree = config.open()?;

        // Start seqno from highest existing seqno + 1, or 0 if empty
        let start_seqno = tree.get_highest_seqno().map(|s| s + 1).unwrap_or(0);

        Ok(Self {
            tree,
            seqno: AtomicU64::new(start_seqno),
            path,
            batch: RefCell::new(None),
        })
    }

    fn next_seqno(&self) -> SeqNo {
        self.seqno.fetch_add(1, Ordering::SeqCst)
    }

    fn current_seqno(&self) -> SeqNo {
        self.seqno.load(Ordering::SeqCst)
    }

    fn has_batch(&self) -> bool {
        self.batch.borrow().is_some()
    }

    fn begin_batch(&self) -> bool {
        let mut batch = self.batch.borrow_mut();
        if batch.is_some() {
            false
        } else {
            *batch = Some(Batch::default());
            true
        }
    }

    fn rollback_batch(&self) -> bool {
        let mut batch = self.batch.borrow_mut();
        if batch.is_some() {
            *batch = None;
            true
        } else {
            false
        }
    }

    fn commit_batch(&self) -> bool {
        let mut batch_ref = self.batch.borrow_mut();
        if let Some(batch) = batch_ref.take() {
            for op in batch.ops {
                match op {
                    BatchOp::Set { key, value } => {
                        let seqno = self.next_seqno();
                        self.tree.insert(key.as_bytes(), value.as_bytes(), seqno);
                    }
                    BatchOp::Del { key } => {
                        let seqno = self.next_seqno();
                        self.tree.remove(key.as_bytes(), seqno);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    fn batch_set(&self, key: String, value: String) {
        let mut batch = self.batch.borrow_mut();
        if let Some(ref mut b) = *batch {
            b.state.insert(key.clone(), Some(value.clone()));
            b.ops.push(BatchOp::Set { key, value });
        }
    }

    fn batch_del(&self, key: String) {
        let mut batch = self.batch.borrow_mut();
        if let Some(ref mut b) = *batch {
            b.state.insert(key.clone(), None);
            b.ops.push(BatchOp::Del { key });
        }
    }

    fn batch_get(&self, key: &str) -> Option<Option<String>> {
        let batch = self.batch.borrow();
        if let Some(ref b) = *batch {
            b.state.get(key).cloned()
        } else {
            None
        }
    }

    fn batch_len(&self) -> usize {
        let batch = self.batch.borrow();
        if let Some(ref b) = *batch {
            b.ops.len()
        } else {
            0
        }
    }
}

fn print_info(session: &Session) {
    // If there's an active batch, show its contents
    let batch = session.batch.borrow();
    if let Some(ref b) = *batch {
        println!("Active batch ({} operations):", b.ops.len());
        for (i, op) in b.ops.iter().enumerate() {
            match op {
                BatchOp::Set { key, value } => {
                    let value_display = if value.len() > 50 {
                        format!("{}...", &value[..50])
                    } else {
                        value.clone()
                    };
                    println!("  {}. SET {} = {}", i + 1, key, value_display);
                }
                BatchOp::Del { key } => {
                    println!("  {}. DEL {}", i + 1, key);
                }
            }
        }
        println!();
    }
    drop(batch);

    println!("Path: {}", session.path.display());
    println!("Tables: {}", session.tree.table_count());
    println!("Approximate items: {}", session.tree.approximate_len());
    println!("Disk space: {} bytes", session.tree.disk_space());
    println!("Sealed memtables: {}", session.tree.sealed_memtable_count());
    println!("Current seqno: {}", session.current_seqno());
    if let Some(seqno) = session.tree.get_highest_seqno() {
        println!("Highest seqno: {}", seqno);
    }
    if let Some(seqno) = session.tree.get_highest_persisted_seqno() {
        println!("Highest persisted seqno: {}", seqno);
    }
    println!("L0 runs: {}", session.tree.l0_run_count());
    for level in 0..7 {
        if let Some(count) = session.tree.level_table_count(level) {
            if count > 0 {
                println!("  L{} tables: {}", level, count);
            }
        }
    }
}

fn handle_get(session: &Session, key: &str) {
    // Check batch first if one exists
    if let Some(batch_value) = session.batch_get(key) {
        match batch_value {
            Some(value) => println!("{}", value),
            None => println!("(deleted in batch)"),
        }
        return;
    }

    match session.tree.get(key.as_bytes(), SeqNo::MAX) {
        Ok(Some(value)) => match std::str::from_utf8(&value) {
            Ok(s) => println!("{}", s),
            Err(_) => println!("{:?}", value.as_ref()),
        },
        Ok(None) => println!("(not found)"),
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn handle_set(session: &Session, key: &str, value: &str, flush: bool) {
    // If batch exists, add to batch instead
    if session.has_batch() {
        session.batch_set(key.to_string(), value.to_string());
        println!("OK (batched, ready to commit)");
        return;
    }

    let seqno = session.next_seqno();
    session.tree.insert(key.as_bytes(), value.as_bytes(), seqno);
    if flush {
        if let Err(e) = session.tree.flush_active_memtable(0) {
            eprintln!("Error flushing: {}", e);
            return;
        }
    }
    println!("OK (set)");
}

fn handle_del(session: &Session, key: &str, weak: bool, flush: bool) {
    // If batch exists and not weak delete, add to batch instead
    if session.has_batch() && !weak {
        session.batch_del(key.to_string());
        println!("OK (batched, ready to commit)");
        return;
    }

    // Note: weak delete is not supported in batches, so we always do direct delete
    let seqno = session.next_seqno();
    if weak {
        session.tree.remove_weak(key.as_bytes(), seqno);
    } else {
        session.tree.remove(key.as_bytes(), seqno);
    }
    if flush {
        if let Err(e) = session.tree.flush_active_memtable(0) {
            eprintln!("Error flushing: {}", e);
            return;
        }
    }
    println!("OK");
}

fn handle_scan(session: &Session, prefix: Option<&str>, long: bool) {
    if session.has_batch() {
        eprintln!("Warning: scan ignores uncommitted batch operations");
    }

    if long {
        // Long mode: iterate over memtable directly to see all internal values
        // including tombstones, then show persisted entries
        handle_scan_long(session, prefix);
    } else {
        // Normal mode: use regular iterator (filters tombstones)
        handle_scan_normal(session, prefix);
    }
}

fn handle_scan_normal(session: &Session, prefix: Option<&str>) {
    let iter: Box<dyn DoubleEndedIterator<Item = _> + Send> = match prefix {
        Some(p) => session.tree.prefix(p.as_bytes(), SeqNo::MAX, None),
        None => session.tree.iter(SeqNo::MAX, None),
    };

    let mut count = 0;
    for item in iter {
        match item.into_inner() {
            Ok((key, value)) => {
                let key_str = String::from_utf8_lossy(&key);
                let value_str = match std::str::from_utf8(&value) {
                    Ok(s) => s.to_string(),
                    Err(_) => format!("{:?}", value.as_ref()),
                };
                println!("{} = {}", key_str, value_str);
                count += 1;
            }
            Err(e) => {
                eprintln!("Error reading item: {}", e);
            }
        }
    }
    println!("OK ({} items)", count);
}

fn format_value_type(vt: ValueType) -> &'static str {
    match vt {
        ValueType::Value => "Value",
        ValueType::Tombstone => "Tombstone",
        ValueType::WeakTombstone => "WeakTombstone",
        ValueType::Indirection => "Indirection",
    }
}

fn handle_scan_long(session: &Session, prefix: Option<&str>) {
    let mut count = 0;
    let mut tombstone_count = 0;

    // First, scan the active memtable (includes tombstones)
    let memtable = session.tree.active_memtable();
    println!("=== Active Memtable ===");
    for item in memtable.iter() {
        let key = &item.key.user_key;
        let key_str = String::from_utf8_lossy(key);

        // Apply prefix filter if specified
        if let Some(p) = prefix {
            if !key_str.starts_with(p) {
                continue;
            }
        }

        let value_type = format_value_type(item.key.value_type);
        let value_len = item.value.len();
        let is_tombstone = item.key.value_type.is_tombstone();

        if is_tombstone {
            println!(
                "{} [len={}, seqno={}, type={}]",
                key_str, value_len, item.key.seqno, value_type
            );
            tombstone_count += 1;
        } else {
            let value_str = match std::str::from_utf8(&item.value) {
                Ok(s) => {
                    if s.len() > 50 {
                        format!("{}...", &s[..50])
                    } else {
                        s.to_string()
                    }
                }
                Err(_) => format!("{:?}", item.value.as_ref()),
            };
            println!(
                "{} = {} [len={}, seqno={}, type={}]",
                key_str, value_str, value_len, item.key.seqno, value_type
            );
        }
        count += 1;
    }

    // Then show persisted entries (from disk)
    println!("\n=== Persisted (on disk) ===");
    let iter: Box<dyn DoubleEndedIterator<Item = _> + Send> = match prefix {
        Some(p) => session.tree.prefix(p.as_bytes(), SeqNo::MAX, None),
        None => session.tree.iter(SeqNo::MAX, None),
    };

    let mut persisted_count = 0;
    for item in iter {
        match item.into_inner() {
            Ok((key, value)) => {
                // Skip if this key is in the memtable (already shown)
                if memtable.get(&key, SeqNo::MAX).is_some() {
                    continue;
                }

                let key_str = String::from_utf8_lossy(&key);
                let value_str = match std::str::from_utf8(&value) {
                    Ok(s) => s.to_string(),
                    Err(_) => format!("{:?}", value.as_ref()),
                };

                // Get internal entry for metadata
                match session.tree.get_internal_entry(&key, SeqNo::MAX) {
                    Ok(Some(internal)) => {
                        let value_type = format_value_type(internal.key.value_type);
                        let value_len = value.len();
                        println!(
                            "{} = {} [len={}, seqno={}, type={}]",
                            key_str, value_str, value_len, internal.key.seqno, value_type
                        );
                    }
                    Ok(None) => {
                        println!("{} = {} [no internal entry]", key_str, value_str);
                    }
                    Err(e) => {
                        println!("{} = {} [error: {}]", key_str, value_str, e);
                    }
                }
                persisted_count += 1;
                count += 1;
            }
            Err(e) => {
                eprintln!("Error reading item: {}", e);
            }
        }
    }

    println!(
        "\n({} total items, {} in memtable, {} persisted, {} tombstones)",
        count,
        count - persisted_count,
        persisted_count,
        tombstone_count
    );

    // Also show tree-level tombstone statistics
    let tree_tombstones = session.tree.tombstone_count();
    let tree_weak_tombstones = session.tree.weak_tombstone_count();
    if tree_tombstones > 0 || tree_weak_tombstones > 0 {
        println!(
            "Tree statistics: {} tombstones, {} weak tombstones (approximate)",
            tree_tombstones, tree_weak_tombstones
        );
    }
}

fn handle_range(session: &Session, start: &str, end: &str) {
    if session.has_batch() {
        eprintln!("Warning: range ignores uncommitted batch operations");
    }

    let iter = session
        .tree
        .range(start.as_bytes()..end.as_bytes(), SeqNo::MAX, None);

    let mut count = 0;
    for item in iter {
        match item.into_inner() {
            Ok((key, value)) => {
                let key_str = String::from_utf8_lossy(&key);
                let value_str = match std::str::from_utf8(&value) {
                    Ok(s) => s.to_string(),
                    Err(_) => format!("{:?}", value.as_ref()),
                };
                println!("{} = {}", key_str, value_str);
                count += 1;
            }
            Err(e) => {
                eprintln!("Error reading item: {}", e);
            }
        }
    }
    println!("({} items)", count);
}

fn handle_count(session: &Session) {
    match session.tree.len(SeqNo::MAX, None) {
        Ok(count) => println!("{}", count),
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn handle_flush(session: &Session) {
    if session.has_batch() {
        eprintln!(
            "WARNING: Active batch ({} operations) still pending",
            session.batch_len()
        );
    }
    match session.tree.flush_active_memtable(0) {
        Ok(()) => println!("OK (flushed)"),
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn handle_compact(session: &Session) {
    match session.tree.major_compact(64 * 1024 * 1024, 0) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("Error: {}", e),
    }
}

/// Result of executing a command
enum CommandResult {
    Continue,
    Exit,
}

/// Execute a parsed command
fn execute_command(session: &Session, cmd: ToolCommand, auto_flush: bool) -> CommandResult {
    match cmd {
        ToolCommand::Get { key } => handle_get(session, &key),
        ToolCommand::Set { key, value } => handle_set(session, &key, &value, auto_flush),
        ToolCommand::Del { key, weak } => handle_del(session, &key, weak, auto_flush),
        ToolCommand::Scan { prefix, long } => handle_scan(session, prefix.as_deref(), long),
        ToolCommand::ScanLong { prefix } => handle_scan_long(session, prefix.as_deref()),
        ToolCommand::Range { start, end } => handle_range(session, &start, &end),
        ToolCommand::Count => handle_count(session),
        ToolCommand::Flush => handle_flush(session),
        ToolCommand::Compact => handle_compact(session),
        ToolCommand::Info => print_info(session),
    }
    CommandResult::Continue
}

/// Execute a shell-only command
fn execute_shell_command(session: &Session, cmd: ShellCommand, auto_flush: bool) -> CommandResult {
    match cmd {
        ShellCommand::ToolCmd(tool_cmd) => execute_command(session, tool_cmd, auto_flush),
        ShellCommand::Exit => {
            if session.has_batch() {
                eprintln!("Warning: discarding uncommitted batch");
                session.rollback_batch();
            }
            handle_flush(session);
            CommandResult::Exit
        }
        ShellCommand::Abort => {
            if session.has_batch() {
                eprintln!("Warning: discarding uncommitted batch");
            }
            CommandResult::Exit
        }
        ShellCommand::Begin => {
            if session.begin_batch() {
                println!("OK (batch started)");
            } else {
                eprintln!("Error: batch already active");
            }
            CommandResult::Continue
        }
        ShellCommand::Commit => {
            if session.commit_batch() {
                println!("OK (batch committed, ready to flush)");
            } else {
                eprintln!("Error: no active batch");
            }
            CommandResult::Continue
        }
        ShellCommand::Rollback => {
            if session.rollback_batch() {
                println!("OK (batch rolled back)");
            } else {
                eprintln!("Error: no active batch");
            }
            CommandResult::Continue
        }
    }
}

/// Parse and run a shell command line
fn run_shell_command(session: &Session, line: &str) -> CommandResult {
    let line = line.trim();
    if line.is_empty() {
        return CommandResult::Continue;
    }

    let tokens = match shlex::split(line) {
        Some(t) if !t.is_empty() => t,
        Some(_) => return CommandResult::Continue,
        None => {
            eprintln!("error: unclosed quote");
            return CommandResult::Continue;
        }
    };

    // Parse remaining commands
    match ShellArgs::try_parse_from(&tokens) {
        Ok(args) => execute_shell_command(session, args.command, false),
        Err(e) => {
            // Print clap's error message
            eprintln!("{}", e);
            CommandResult::Continue
        }
    }
}

fn run_shell(session: &Session) {
    if io::stdin().is_terminal() {
        run_shell_interactive(session);
    } else {
        run_shell_non_interactive(session);
    }
}

fn run_shell_interactive(session: &Session) {
    println!("Welcome to the LSM-tree shell");
    println!("Type 'help' for available commands, 'exit' to quit.\n");

    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("Error initializing line editor: {}", e);
            return;
        }
    };

    loop {
        match rl.readline("lsm> ") {
            Ok(line) => {
                rl.add_history_entry(&line);
                if let CommandResult::Exit = run_shell_command(session, &line) {
                    break;
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                // Ignore Ctrl+C, just show a new prompt
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                break;
            }
        }
    }
}

fn run_shell_non_interactive(session: &Session) {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        if stdout.flush().is_err() {
            die!("can't flush stdout");
        }

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                // EOF
                break;
            }
            Ok(_) => {
                if let CommandResult::Exit = run_shell_command(session, &line) {
                    break;
                }
            }
            Err(e) => {
                die!("Error reading input: {}", e);
            }
        }
    }
}

fn main() {
    let args = ToolArgs::parse();
    let (verbose, level_filter) = init_tracing(args.quiet, args.verbose);

    let cmd = ToolArgs::command();

    info!(
        "starting {} ({} {}), log level: {level_filter}",
        cmd.get_name(),
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let session = match Session::open(args.lsm_path, args.separation_threshold, args.blob_tree) {
        Ok(s) => s,
        Err(e) => {
            let note = if verbose {
                ""
            } else {
                ". Note: Use -v (one or multiple times) for more information"
            };
            die!("Error opening tree: {}{}", e, note);
        }
    };

    match args.command {
        Some(cmd) => {
            execute_command(&session, cmd, true);
        }
        None => run_shell(&session),
    }
}
