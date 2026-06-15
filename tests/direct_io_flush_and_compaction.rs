// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Integration tests covering the `use_direct_io_for_compaction_reads` and
//! `use_direct_io_for_flush_and_compaction` config knobs.
//!
//! Each test exercises a full flush + compaction round-trip with one or both
//! direct-I/O knobs enabled and then verifies the data is readable and correct.
//! Covered permutations: writes-only, reads-only, both, and the buffered baseline.

use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
#[cfg(all(target_os = "linux", debug_assertions))]
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};
use test_log::test;

fn populate_keys(
    tree: &lsm_tree::AnyTree,
    range: std::ops::Range<u32>,
    seqno: &SequenceNumberCounter,
) {
    for i in range {
        let key = format!("k{i:08}");
        let value = format!("v{i:08}_payload_data_to_make_values_a_bit_larger");
        tree.insert(key.as_bytes(), value.as_bytes(), seqno.next());
    }
}

fn verify_keys(tree: &lsm_tree::AnyTree, range: std::ops::Range<u32>) -> lsm_tree::Result<()> {
    for i in range {
        let key = format!("k{i:08}");
        let expected = format!("v{i:08}_payload_data_to_make_values_a_bit_larger");
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(expected.as_bytes()),
            "key {key} did not round-trip",
        );
    }
    Ok(())
}

// Builds a config with the two direct-I/O knobs set as requested. Returns the
// seqno counter so each test can pass it to `populate_keys` to keep the global
// counter consistent with what the tree sees on flush/compact.
fn config_with(
    folder: &std::path::Path,
    direct_writes: bool,
    direct_reads: bool,
) -> (Config, SequenceNumberCounter) {
    let seqno = SequenceNumberCounter::default();
    let cfg = Config::new(folder, seqno.clone(), SequenceNumberCounter::default())
        .use_direct_io_for_flush_and_compaction(direct_writes)
        .use_direct_io_for_compaction_reads(direct_reads);
    (cfg, seqno)
}

#[cfg(all(target_os = "linux", debug_assertions))]
const TEST_MAX_READ_BYTES_ENV: &str = "LSM_TREE_TEST_DIRECT_IO_MAX_READ_BYTES";

#[cfg(all(target_os = "linux", debug_assertions))]
struct ForcedDirectReadLimitGuard {
    previous: Option<OsString>,
}

#[cfg(all(target_os = "linux", debug_assertions))]
impl ForcedDirectReadLimitGuard {
    fn set(max_read_bytes: usize) -> Self {
        let previous = std::env::var_os(TEST_MAX_READ_BYTES_ENV);
        std::env::set_var(TEST_MAX_READ_BYTES_ENV, max_read_bytes.to_string());
        Self { previous }
    }
}

#[cfg(all(target_os = "linux", debug_assertions))]
impl Drop for ForcedDirectReadLimitGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            std::env::set_var(TEST_MAX_READ_BYTES_ENV, previous);
        } else {
            std::env::remove_var(TEST_MAX_READ_BYTES_ENV);
        }
    }
}

#[cfg(all(target_os = "linux", debug_assertions))]
fn direct_io_alignment_for_test() -> usize {
    // SAFETY: sysconf is called with a constant and has no aliasing requirements.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    match usize::try_from(page_size) {
        Ok(v) if v >= 512 && v.is_power_of_two() => v,
        _ => 4_096,
    }
}

#[cfg(all(target_os = "linux", debug_assertions))]
fn filesystem_accepts_direct_io(dir: &Path, alignment: usize) -> std::io::Result<bool> {
    let path = dir.join(".direct_io_probe");
    {
        let mut file = File::create(&path)?;
        file.write_all(&vec![0; alignment])?;
        file.sync_all()?;
    }

    let opened = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path);
    let _ = std::fs::remove_file(&path);

    match opened {
        Ok(_) => Ok(true),
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => Ok(false),
        Err(e) => Err(e),
    }
}

#[test]
fn direct_io_buffered_baseline_roundtrip() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Sanity baseline so the "direct I/O on" tests below have a comparable shape.
    populate_keys(&tree, 0..500, &seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    verify_keys(&tree, 0..500)?;
    Ok(())
}

#[test]
fn direct_io_writes_only_flush_and_compact() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, false);
    let tree = cfg.open()?;

    populate_keys(&tree, 0..500, &seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;

    verify_keys(&tree, 0..500)?;
    Ok(())
}

#[test]
fn direct_io_reads_only_flush_and_compact() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), false, true);
    let tree = cfg.open()?;

    populate_keys(&tree, 0..500, &seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;

    verify_keys(&tree, 0..500)?;
    Ok(())
}

#[test]
fn direct_io_reads_and_writes_flush_and_compact() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;

    populate_keys(&tree, 0..500, &seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;

    verify_keys(&tree, 0..500)?;
    Ok(())
}

#[cfg(all(target_os = "linux", debug_assertions))]
#[test]
fn direct_io_compaction_reads_continue_after_aligned_short_read_before_eof() -> lsm_tree::Result<()>
{
    use lsm_tree::config::CompressionPolicy;
    use lsm_tree::CompressionType;

    let folder = get_tmp_folder();
    let alignment = direct_io_alignment_for_test();
    if !filesystem_accepts_direct_io(folder.path(), alignment)? {
        eprintln!("filesystem does not support direct I/O; skipping short-read assertion");
        return Ok(());
    }

    let _guard = ForcedDirectReadLimitGuard::set(alignment);
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .use_direct_io_for_compaction_reads(true)
    .open()?;

    for batch in 0..4 {
        let start = batch * 700;
        let end = start + 700;
        for i in start..end {
            let key = format!("k{i:08}");
            let value = format!("v{i:08}_short_read_payload_").repeat(12);
            tree.insert(key.as_bytes(), value.as_bytes(), seqno.next());
        }
        tree.flush_active_memtable(0)?;
    }

    tree.major_compact(128 * 1_024, 0)?;

    for i in 0..2_800 {
        let key = format!("k{i:08}");
        let expected = format!("v{i:08}_short_read_payload_").repeat(12);
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(expected.as_bytes()),
            "key {key} did not round-trip after forced short direct reads",
        );
    }
    Ok(())
}

#[test]
fn direct_io_multiple_flushes_then_compaction() -> lsm_tree::Result<()> {
    // Exercises rotation across multiple SSTs in both flush + compaction with the
    // direct I/O paths active.
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;

    for batch in 0..5 {
        let lo = batch * 200;
        let hi = lo + 200;
        populate_keys(&tree, lo..hi, &seqno);
        tree.flush_active_memtable(0)?;
    }

    assert!(tree.table_count() >= 5);

    tree.major_compact(64 * 1_024 * 1_024, 0)?;

    verify_keys(&tree, 0..1_000)?;
    Ok(())
}

#[test]
fn direct_io_with_tombstones() -> lsm_tree::Result<()> {
    // Tombstone propagation through a direct-I/O compaction must still drop the
    // corresponding inserts on the last level.
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;

    populate_keys(&tree, 0..200, &seqno);
    tree.flush_active_memtable(0)?;

    for i in 0..100 {
        let key = format!("k{i:08}");
        tree.remove(key.as_bytes(), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64 * 1_024 * 1_024, seqno.get())?;

    for i in 0..100 {
        let key = format!("k{i:08}");
        assert!(
            tree.get(key.as_bytes(), SeqNo::MAX)?.is_none(),
            "removed {key} should not exist",
        );
    }
    verify_keys(&tree, 100..200)?;
    Ok(())
}

#[cfg(feature = "lz4")]
#[test]
fn direct_io_with_lz4_compression() -> lsm_tree::Result<()> {
    use lsm_tree::config::CompressionPolicy;
    use lsm_tree::CompressionType;

    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
    .use_direct_io_for_flush_and_compaction(true)
    .use_direct_io_for_compaction_reads(true)
    .open()?;

    populate_keys(&tree, 0..500, &seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    verify_keys(&tree, 0..500)?;
    Ok(())
}

#[test]
fn direct_io_blob_tree_flush_and_compact() -> lsm_tree::Result<()> {
    // Blob tree exercises both SST and blob-file writers, plus the blob scanner
    // during relocation. All three direct-I/O paths must produce correct data.
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(8)))
    .use_direct_io_for_flush_and_compaction(true)
    .use_direct_io_for_compaction_reads(true)
    .open()?;

    // Values are larger than the separation threshold so they go to blob files.
    for i in 0..200 {
        let key = format!("blobkey_{i:06}");
        let value = format!("blobval_{i:06}_").repeat(20); // ~280 bytes per value
        tree.insert(key.as_bytes(), value.as_bytes(), seqno.next());
    }

    tree.flush_active_memtable(0)?;
    assert!(tree.blob_file_count() >= 1);

    tree.major_compact(64 * 1_024 * 1_024, 0)?;

    for i in 0..200 {
        let key = format!("blobkey_{i:06}");
        let expected = format!("blobval_{i:06}_").repeat(20);
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(expected.as_bytes()),
            "blob {key} did not round-trip"
        );
    }
    Ok(())
}

#[test]
fn direct_io_blob_relocation_runs_scanner() -> lsm_tree::Result<()> {
    // Overwriting a separated value makes its original blob file stale; the next
    // major compaction relocates the surviving entries out of that file via
    // BlobFileScanner. With both direct-I/O knobs on, this runs the scanner's
    // manual offset-summation (which replaced stream_position) under direct I/O,
    // which the no-overwrite blob test above never does. Mirrors
    // blob_tree_major_compact_relocation_simple, plus a value round-trip check.
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().age_cutoff(1.0)))
    .use_direct_io_for_flush_and_compaction(true)
    .use_direct_io_for_compaction_reads(true)
    .open()?;

    let big = b"neptune!".repeat(2_000); // ~16 KiB -> separated into a blob file
    let new_big = b"winter!".repeat(2_000);

    tree.insert("big", &big, seqno.next());
    tree.insert("big2", &big, seqno.next());
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.blob_file_count(), 1);

    // Overwrite "big": its original blob entry becomes garbage, making the first
    // blob file stale (1 of 2 entries dead = 50% >= default staleness threshold).
    tree.insert("big", &new_big, seqno.next());
    tree.flush_active_memtable(0)?;

    tree.major_compact(64 * 1_024 * 1_024, seqno.next())?;
    // First blob dropped; its live entry ("big2") relocated into a fresh blob,
    // alongside the second-generation "big" blob => 2 blob files.
    assert_eq!(tree.blob_file_count(), 2);

    assert_eq!(
        tree.get("big", SeqNo::MAX)?.as_deref(),
        Some(new_big.as_slice()),
        "overwritten value did not round-trip",
    );
    assert_eq!(
        tree.get("big2", SeqNo::MAX)?.as_deref(),
        Some(big.as_slice()),
        "relocated value did not round-trip",
    );
    Ok(())
}

#[test]
fn direct_io_minor_compaction_with_partial_block_tail() -> lsm_tree::Result<()> {
    // Stress the AlignedFileWriter's trailing-block padding+truncate path: write a
    // small enough payload that the on-disk file size is much less than the page
    // alignment unit.
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;

    // A handful of tiny KVs: the entire SST tail (incl. meta) will be < 1 page.
    for i in 0..3 {
        tree.insert(
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
            seqno.next(),
        );
    }
    tree.flush_active_memtable(0)?;

    for i in 0..3 {
        let key = format!("k{i}");
        let expected = format!("v{i}");
        assert_eq!(
            tree.get(key.as_bytes(), SeqNo::MAX)?.as_deref(),
            Some(expected.as_bytes()),
        );
    }
    Ok(())
}

#[test]
fn direct_io_knob_compatible_with_drop_range() -> lsm_tree::Result<()> {
    // drop_range goes through the Choice::Drop compaction path, not Merge, so it
    // never opens compaction-input files. This test only confirms that enabling the
    // direct-I/O knob does not surprise the drop path.
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;

    populate_keys(&tree, 0..100, &seqno);
    tree.flush_active_memtable(0)?;
    let table_count_before = tree.table_count();
    assert!(table_count_before >= 1);

    // Fully contains every k00000000..k00000099 produced above.
    tree.drop_range::<&[u8], _>(..)?;
    assert_eq!(0, tree.table_count(), "all tables should have been dropped");
    for i in 0..100 {
        let key = format!("k{i:08}");
        assert!(tree.get(key.as_bytes(), SeqNo::MAX)?.is_none());
    }
    Ok(())
}

#[test]
fn direct_io_reopen_persists_data() -> lsm_tree::Result<()> {
    // Direct I/O is purely a runtime knob; the on-disk format is identical. A
    // database written with direct I/O on must be readable when reopened with the
    // knob off, and vice versa.
    let folder = get_tmp_folder();

    {
        let (cfg, seqno) = config_with(folder.path(), true, true);
        let tree = cfg.open()?;
        populate_keys(&tree, 0..200, &seqno);
        tree.flush_active_memtable(0)?;
        tree.major_compact(64 * 1_024 * 1_024, 0)?;
    }

    // Reopen with the knob off; data must be intact.
    {
        let (cfg, _seqno) = config_with(folder.path(), false, false);
        let tree = cfg.open()?;
        verify_keys(&tree, 0..200)?;
    }

    Ok(())
}

#[test]
fn direct_io_multi_pass_compaction() -> lsm_tree::Result<()> {
    // Multiple compaction passes exercise the read+write paths repeatedly with the
    // same file handles, catching state-corruption issues that single-pass tests miss.
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;

    for pass in 0..3 {
        let base = pass * 300;
        populate_keys(&tree, base..(base + 300), &seqno);
        tree.flush_active_memtable(0)?;
        tree.compact(
            Arc::new(lsm_tree::compaction::Fifo::new(64 * 1_024 * 1_024, None)),
            0,
        )?;
    }

    verify_keys(&tree, 0..900)?;
    Ok(())
}

#[test]
fn direct_io_multi_writer_rotation_with_direct_io() -> lsm_tree::Result<()> {
    // Drives a single major compaction that produces enough output to force
    // `MultiWriter::rotate` (which re-opens a fresh direct-I/O file mid-compaction)
    // by setting a small target_size. This is the missing-rotation-test gap surfaced
    // by the review.
    //
    // Compression is explicitly disabled because with LZ4 the highly-repetitive
    // payload compresses well enough to fit a single table.
    use lsm_tree::config::CompressionPolicy;
    use lsm_tree::CompressionType;

    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .use_direct_io_for_flush_and_compaction(true)
    .use_direct_io_for_compaction_reads(true)
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()?;

    // Each value is ~256 bytes; 8_000 keys = ~2 MiB of user data, easily forcing
    // multiple rotations with the 256 KiB target_size below.
    let value = "v_payload_padding_padding_padding_padding_padding_padding".repeat(4);
    for i in 0..8_000_u32 {
        let key = format!("k{i:08}");
        tree.insert(key.as_bytes(), value.as_bytes(), seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // major_compact with small target_size triggers MultiWriter::rotate.
    tree.major_compact(256 * 1_024, 0)?;
    assert!(
        tree.table_count() >= 2,
        "compaction with small target_size should produce multiple output tables, got {}",
        tree.table_count(),
    );

    for i in 0..8_000_u32 {
        let key = format!("k{i:08}");
        let got = tree.get(key.as_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(value.as_bytes()),
            "key {key} did not round-trip after rotation",
        );
    }
    Ok(())
}

#[test]
fn direct_io_falls_back_on_unsupported_filesystem() -> lsm_tree::Result<()> {
    // The knob is documented as silently falling back to buffered I/O on
    // filesystems that reject direct I/O (e.g. tmpfs/overlayfs/FUSE). On macOS
    // F_NOCACHE is universally accepted, on Linux/Windows the FS may reject;
    // either way the tree must operate correctly and the data must round-trip.
    let folder = get_tmp_folder();
    let (cfg, seqno) = config_with(folder.path(), true, true);
    let tree = cfg.open()?;
    populate_keys(&tree, 0..100, &seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    verify_keys(&tree, 0..100)?;
    Ok(())
}
