// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Encoder for range tombstone blocks.
//!
//! Two layouts are supported:
//! - **ByStart**: sorted by `(start asc, seqno desc, end asc)`, prefix-compresses START keys.
//!   Includes per-window `max_end` for pruning during queries.
//! - **ByEndDesc**: sorted by `(end desc, seqno desc)`, prefix-compresses END keys.
//!   Used for reverse iteration only; no per-window metadata.

use crate::range_tombstone::RangeTombstone;
use crate::UserKey;
use byteorder::{LittleEndian, WriteBytesExt};
use varint_rs::VarintWriter;

/// Default restart interval for range tombstone blocks.
pub const RESTART_INTERVAL: usize = 16;

/// Encodes range tombstones sorted by `(start asc, seqno desc, end asc)`.
///
/// Output format (backward-parseable):
/// ```text
/// [entries][window_max_ends_blob][global_max_end][global_max_end_len:u16]
/// [max_seqno:u64][window_max_ends_bytes_len:u32][window_max_ends_count:u32]
/// [restart_offsets: count * u32][count:u32][restart_count:u32]
/// ```
pub fn encode_by_start(tombstones: &[RangeTombstone]) -> Vec<u8> {
    let mut buf = Vec::new();

    if tombstones.is_empty() {
        // Empty block: just trailer
        write_empty_by_start_footer(&mut buf);
        return buf;
    }

    let mut restart_offsets: Vec<u32> = Vec::new();
    let mut window_max_ends: Vec<UserKey> = Vec::new();
    let mut current_window_max_end: Option<UserKey> = None;
    let mut prev_start: Option<UserKey> = None;
    let mut max_seqno: u64 = 0;
    #[expect(clippy::expect_used, reason = "non-empty tombstones checked above")]
    let mut global_max_end: UserKey = tombstones
        .first()
        .expect("non-empty")
        .end
        .clone();

    for (i, rt) in tombstones.iter().enumerate() {
        if rt.seqno > max_seqno {
            max_seqno = rt.seqno;
        }
        if rt.end > global_max_end {
            global_max_end = rt.end.clone();
        }

        let is_restart = i % RESTART_INTERVAL == 0;

        if is_restart {
            // Finalize previous window
            if let Some(max_end) = current_window_max_end.take() {
                window_max_ends.push(max_end);
            }

            #[expect(
                clippy::cast_possible_truncation,
                reason = "block size fits in u32"
            )]
            restart_offsets.push(buf.len() as u32);
            current_window_max_end = Some(rt.end.clone());

            // Full entry: shared_prefix_len = 0
            write_varint_usize(&mut buf, 0); // shared_prefix_len
            write_varint_usize(&mut buf, rt.start.len()); // start_suffix_len
            buf.extend_from_slice(rt.start.as_ref()); // start_suffix (= full start)
            write_varint_usize(&mut buf, rt.end.len()); // end_key_len
            buf.extend_from_slice(rt.end.as_ref()); // end_key
            write_varint_u64(&mut buf, rt.seqno); // seqno

            prev_start = Some(rt.start.clone());
        } else {
            // Truncated entry with prefix compression on start
            let prev = prev_start.as_ref().expect("must have prev after restart");
            let shared = common_prefix_len(prev.as_ref(), rt.start.as_ref());

            write_varint_usize(&mut buf, shared); // shared_prefix_len
            write_varint_usize(&mut buf, rt.start.len() - shared); // start_suffix_len
            #[expect(
                clippy::indexing_slicing,
                reason = "shared <= rt.start.len()"
            )]
            buf.extend_from_slice(&rt.start.as_ref()[shared..]); // start_suffix
            write_varint_usize(&mut buf, rt.end.len()); // end_key_len (always full)
            buf.extend_from_slice(rt.end.as_ref()); // end_key
            write_varint_u64(&mut buf, rt.seqno); // seqno

            // Update window max_end
            if let Some(ref mut max_end) = current_window_max_end {
                if rt.end > *max_end {
                    *max_end = rt.end.clone();
                }
            }

            prev_start = Some(rt.start.clone());
        }
    }

    // Finalize last window
    if let Some(max_end) = current_window_max_end.take() {
        window_max_ends.push(max_end);
    }

    debug_assert_eq!(
        window_max_ends.len(),
        restart_offsets.len(),
        "window_max_ends_count must equal restart_count"
    );

    // Write window max_ends blob
    let window_max_ends_start = buf.len();
    for max_end in &window_max_ends {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "key length bounded by u16"
        )]
        let len = max_end.len() as u16;
        buf.write_u16::<LittleEndian>(len).expect("write to vec");
        buf.extend_from_slice(max_end.as_ref());
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "blob size fits in u32"
    )]
    let window_max_ends_bytes_len = (buf.len() - window_max_ends_start) as u32;

    // Write global_max_end
    buf.extend_from_slice(global_max_end.as_ref());

    // Write global_max_end_len
    #[expect(
        clippy::cast_possible_truncation,
        reason = "key length bounded by u16"
    )]
    let global_max_end_len = global_max_end.len() as u16;
    buf.write_u16::<LittleEndian>(global_max_end_len)
        .expect("write to vec");

    // Write max_seqno
    buf.write_u64::<LittleEndian>(max_seqno)
        .expect("write to vec");

    // Write window_max_ends_bytes_len
    buf.write_u32::<LittleEndian>(window_max_ends_bytes_len)
        .expect("write to vec");

    // Write window_max_ends_count
    #[expect(
        clippy::cast_possible_truncation,
        reason = "restart count fits in u32"
    )]
    let window_max_ends_count = window_max_ends.len() as u32;
    buf.write_u32::<LittleEndian>(window_max_ends_count)
        .expect("write to vec");

    // Write restart offsets
    for offset in &restart_offsets {
        buf.write_u32::<LittleEndian>(*offset)
            .expect("write to vec");
    }

    // Write trailer: count, restart_count
    #[expect(
        clippy::cast_possible_truncation,
        reason = "tombstone count fits in u32"
    )]
    let count = tombstones.len() as u32;
    buf.write_u32::<LittleEndian>(count).expect("write to vec");

    #[expect(
        clippy::cast_possible_truncation,
        reason = "restart count fits in u32"
    )]
    let restart_count = restart_offsets.len() as u32;
    buf.write_u32::<LittleEndian>(restart_count)
        .expect("write to vec");

    buf
}

/// Encodes range tombstones sorted by `(end desc, seqno desc)`.
///
/// Output format (backward-parseable):
/// ```text
/// [entries][global_max_end][max_end_len:u16][max_seqno:u64]
/// [restart_offsets: count * u32][count:u32][restart_count:u32]
/// ```
pub fn encode_by_end_desc(tombstones: &[RangeTombstone]) -> Vec<u8> {
    let mut buf = Vec::new();

    if tombstones.is_empty() {
        write_empty_by_end_footer(&mut buf);
        return buf;
    }

    let mut restart_offsets: Vec<u32> = Vec::new();
    let mut prev_end: Option<UserKey> = None;
    let mut max_seqno: u64 = 0;
    // Since sorted by end desc, first entry has the max end
    #[expect(clippy::expect_used, reason = "non-empty tombstones checked above")]
    let global_max_end = tombstones
        .first()
        .expect("non-empty")
        .end
        .clone();

    for (i, rt) in tombstones.iter().enumerate() {
        if rt.seqno > max_seqno {
            max_seqno = rt.seqno;
        }

        let is_restart = i % RESTART_INTERVAL == 0;

        if is_restart {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "block size fits in u32"
            )]
            restart_offsets.push(buf.len() as u32);

            // Full entry: shared_prefix_len = 0
            write_varint_usize(&mut buf, 0); // shared_prefix_len
            write_varint_usize(&mut buf, rt.end.len()); // end_suffix_len
            buf.extend_from_slice(rt.end.as_ref()); // end_suffix (= full end)
            write_varint_usize(&mut buf, rt.start.len()); // start_key_len (always full)
            buf.extend_from_slice(rt.start.as_ref()); // start_key
            write_varint_u64(&mut buf, rt.seqno); // seqno

            prev_end = Some(rt.end.clone());
        } else {
            // Prefix-compress END keys
            let prev = prev_end.as_ref().expect("must have prev after restart");
            let shared = common_prefix_len(prev.as_ref(), rt.end.as_ref());

            write_varint_usize(&mut buf, shared); // shared_prefix_len
            write_varint_usize(&mut buf, rt.end.len() - shared); // end_suffix_len
            #[expect(
                clippy::indexing_slicing,
                reason = "shared <= rt.end.len()"
            )]
            buf.extend_from_slice(&rt.end.as_ref()[shared..]); // end_suffix
            write_varint_usize(&mut buf, rt.start.len()); // start_key_len (always full)
            buf.extend_from_slice(rt.start.as_ref()); // start_key
            write_varint_u64(&mut buf, rt.seqno); // seqno

            prev_end = Some(rt.end.clone());
        }
    }

    // Write global_max_end
    buf.extend_from_slice(global_max_end.as_ref());

    // Write max_end_len
    #[expect(
        clippy::cast_possible_truncation,
        reason = "key length bounded by u16"
    )]
    let max_end_len = global_max_end.len() as u16;
    buf.write_u16::<LittleEndian>(max_end_len)
        .expect("write to vec");

    // Write max_seqno
    buf.write_u64::<LittleEndian>(max_seqno)
        .expect("write to vec");

    // Write restart offsets
    for offset in &restart_offsets {
        buf.write_u32::<LittleEndian>(*offset)
            .expect("write to vec");
    }

    // Write trailer
    #[expect(
        clippy::cast_possible_truncation,
        reason = "tombstone count fits in u32"
    )]
    let count = tombstones.len() as u32;
    buf.write_u32::<LittleEndian>(count).expect("write to vec");

    #[expect(
        clippy::cast_possible_truncation,
        reason = "restart count fits in u32"
    )]
    let restart_count = restart_offsets.len() as u32;
    buf.write_u32::<LittleEndian>(restart_count)
        .expect("write to vec");

    buf
}

fn write_empty_by_start_footer(buf: &mut Vec<u8>) {
    // global_max_end_len = 0
    buf.write_u16::<LittleEndian>(0).expect("write to vec");
    // max_seqno = 0
    buf.write_u64::<LittleEndian>(0).expect("write to vec");
    // window_max_ends_bytes_len = 0
    buf.write_u32::<LittleEndian>(0).expect("write to vec");
    // window_max_ends_count = 0
    buf.write_u32::<LittleEndian>(0).expect("write to vec");
    // count = 0, restart_count = 0
    buf.write_u32::<LittleEndian>(0).expect("write to vec");
    buf.write_u32::<LittleEndian>(0).expect("write to vec");
}

fn write_empty_by_end_footer(buf: &mut Vec<u8>) {
    // max_end_len = 0
    buf.write_u16::<LittleEndian>(0).expect("write to vec");
    // max_seqno = 0
    buf.write_u64::<LittleEndian>(0).expect("write to vec");
    // count = 0, restart_count = 0
    buf.write_u32::<LittleEndian>(0).expect("write to vec");
    buf.write_u32::<LittleEndian>(0).expect("write to vec");
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

fn write_varint_usize(buf: &mut Vec<u8>, val: usize) {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "varint values are bounded by u16/u32"
    )]
    buf.write_u32_varint(val as u32).expect("write to vec");
}

fn write_varint_u64(buf: &mut Vec<u8>, val: u64) {
    buf.write_u64_varint(val).expect("write to vec");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserKey;

    fn rt(start: &[u8], end: &[u8], seqno: u64) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    #[test]
    fn encode_empty_by_start() {
        let data = encode_by_start(&[]);
        // Should have the empty footer
        assert!(!data.is_empty());
    }

    #[test]
    fn encode_empty_by_end_desc() {
        let data = encode_by_end_desc(&[]);
        assert!(!data.is_empty());
    }

    #[test]
    fn encode_single_by_start() {
        let tombstones = vec![rt(b"a", b"z", 10)];
        let data = encode_by_start(&tombstones);
        assert!(data.len() > 8); // at least trailer
    }

    #[test]
    fn encode_multiple_by_start_with_prefix_compression() {
        let tombstones = vec![
            rt(b"abc", b"def", 10),
            rt(b"abd", b"ghi", 5),
        ];
        let data = encode_by_start(&tombstones);
        assert!(data.len() > 8);
    }

    #[test]
    fn encode_by_start_restart_points() {
        // More than RESTART_INTERVAL entries to trigger multiple windows
        let mut tombstones = Vec::new();
        for i in 0u8..40 {
            let start = vec![b'a', i];
            let end = vec![b'z', i];
            tombstones.push(rt(&start, &end, u64::from(i)));
        }
        let data = encode_by_start(&tombstones);
        assert!(data.len() > 8);
    }

    #[test]
    fn encode_single_by_end_desc() {
        let tombstones = vec![rt(b"a", b"z", 10)];
        let data = encode_by_end_desc(&tombstones);
        assert!(data.len() > 8);
    }
}
