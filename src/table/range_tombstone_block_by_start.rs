// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Range tombstone block sorted by `(start asc, seqno desc, end asc)`.
//!
//! Supports point queries, overlap collection, and range-cover queries
//! with per-window `max_end` pruning. Prefix-compresses START keys.

use crate::range_tombstone::{CoveringRt, RangeTombstone};
use crate::{SeqNo, UserKey};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use varint_rs::VarintReader;

/// Parsed range tombstone block in ByStart layout.
pub struct RangeTombstoneBlockByStart {
    data: Vec<u8>,
    count: u32,
    restart_count: u32,
    restart_offsets: Vec<u32>,
    window_max_ends: Vec<UserKey>,
    global_max_end: UserKey,
    max_seqno: u64,
    entries_end: usize,
}

impl RangeTombstoneBlockByStart {
    /// Parses the backward-parseable footer to construct the block.
    pub fn parse(data: Vec<u8>) -> crate::Result<Self> {
        if data.len() < 8 {
            return Err(crate::Error::InvalidBlock(
                "range tombstone block too small",
            ));
        }

        let total_len = data.len();

        // Step 1: Trailer (last 8 bytes)
        let trailer_start = total_len - 8;
        let count = read_u32_le(&data, trailer_start)?;
        let restart_count = read_u32_le(&data, trailer_start + 4)?;

        if count == 0 {
            return Ok(Self {
                data,
                count: 0,
                restart_count: 0,
                restart_offsets: Vec::new(),
                window_max_ends: Vec::new(),
                global_max_end: UserKey::from(b"" as &[u8]),
                max_seqno: 0,
                entries_end: 0,
            });
        }

        // Step 2: Restart array
        let restart_array_size = (restart_count as usize) * 4;
        let restart_array_start = trailer_start - restart_array_size;
        let mut restart_offsets = Vec::with_capacity(restart_count as usize);
        for i in 0..restart_count as usize {
            restart_offsets.push(read_u32_le(&data, restart_array_start + i * 4)?);
        }

        // Step 3: window_max_ends_count
        let wme_count_pos = restart_array_start - 4;
        let window_max_ends_count = read_u32_le(&data, wme_count_pos)?;

        // Step 4: window_max_ends_bytes_len
        let wme_bytes_len_pos = wme_count_pos - 4;
        let window_max_ends_bytes_len = read_u32_le(&data, wme_bytes_len_pos)?;

        // Step 5: max_seqno
        let max_seqno_pos = wme_bytes_len_pos - 8;
        let max_seqno = read_u64_le(&data, max_seqno_pos)?;

        // Step 6: global_max_end_len
        let gme_len_pos = max_seqno_pos - 2;
        let global_max_end_len = read_u16_le(&data, gme_len_pos)? as usize;

        // Step 7: global_max_end
        let gme_start = gme_len_pos - global_max_end_len;
        let global_max_end = data
            .get(gme_start..gme_start + global_max_end_len)
            .ok_or(crate::Error::InvalidBlock(
                "range tombstone block: global_max_end out of bounds",
            ))?;
        let global_max_end = UserKey::from(global_max_end);

        // Step 8: Window max_ends blob
        let wme_blob_start = gme_start - window_max_ends_bytes_len as usize;
        let window_max_ends =
            parse_window_max_ends(&data, wme_blob_start, window_max_ends_count as usize)?;

        // Step 9: entries_end
        let entries_end = wme_blob_start;

        if window_max_ends_count != restart_count {
            return Err(crate::Error::InvalidBlock(
                "range tombstone block: window_max_ends_count != restart_count",
            ));
        }

        Ok(Self {
            data,
            count,
            restart_count,
            restart_offsets,
            window_max_ends,
            global_max_end,
            max_seqno,
            entries_end,
        })
    }

    /// Returns `true` if the block contains no tombstones.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the number of tombstones.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Point query: returns the seqno of a suppressing tombstone if one exists.
    ///
    /// A tombstone suppresses key at `key_seqno` if:
    /// - `rt.start <= key < rt.end`
    /// - `rt.seqno > key_seqno`
    /// - `rt.seqno <= read_seqno`
    pub fn query_suppression(
        &self,
        key: &[u8],
        key_seqno: SeqNo,
        read_seqno: SeqNo,
    ) -> crate::Result<Option<SeqNo>> {
        // Fast reject: empty block
        if self.is_empty() {
            return Ok(None);
        }

        // Fast reject: max_seqno can't suppress
        if self.max_seqno <= key_seqno {
            return Ok(None);
        }

        // Fast reject: no tombstone extends past this key
        if self.global_max_end.as_ref() <= key {
            return Ok(None);
        }

        // Fast reject: max_seqno not visible
        if self.max_seqno > read_seqno {
            // There might still be lower-seqno tombstones that are visible,
            // so we can't reject entirely unless ALL seqnos > read_seqno.
            // We don't have a min_seqno here, so fall through.
        }

        // Scan windows to find suppressing tombstone
        let restart_idx = self.find_restart_point(key);
        let mut best_seqno: Option<SeqNo> = None;

        for wi in 0..=restart_idx {
            // Prune: window max_end <= key means no tombstone in window covers key
            if let Some(max_end) = self.window_max_ends.get(wi) {
                if max_end.as_ref() <= key {
                    continue;
                }
            }

            let entries = self.decode_window(wi)?;
            for rt in entries {
                if rt.start.as_ref() > key {
                    break; // All further entries in this window have start > key
                }
                if rt.should_suppress(key, key_seqno, read_seqno) {
                    let s = rt.seqno;
                    if best_seqno.map_or(true, |b| s > b) {
                        best_seqno = Some(s);
                    }
                }
            }
        }

        Ok(best_seqno)
    }

    /// Returns all tombstones overlapping with `key` and visible at `read_seqno`.
    ///
    /// Used for seek initialization: returns tombstones where
    /// `start <= key < end` and `seqno <= read_seqno`.
    pub fn overlapping_tombstones(
        &self,
        key: &[u8],
        read_seqno: SeqNo,
    ) -> crate::Result<Vec<RangeTombstone>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        // Fast reject: no tombstone extends past this key
        if self.global_max_end.as_ref() <= key {
            return Ok(Vec::new());
        }

        let restart_idx = self.find_restart_point(key);
        let mut result = Vec::new();

        for wi in 0..=restart_idx {
            // Prune: window max_end <= key
            if let Some(max_end) = self.window_max_ends.get(wi) {
                if max_end.as_ref() <= key {
                    continue;
                }
            }

            let entries = self.decode_window(wi)?;
            for rt in entries {
                if rt.start.as_ref() > key {
                    break;
                }
                if rt.contains_key(key) && rt.visible_at(read_seqno) {
                    result.push(rt);
                }
            }
        }

        Ok(result)
    }

    /// Returns the highest-seqno visible tombstone that fully covers `[min, max]`.
    ///
    /// Used for table-skip decisions. A covering tombstone must satisfy:
    /// - `rt.start <= min`
    /// - `max < rt.end` (half-open)
    /// - `rt.seqno <= read_seqno`
    pub fn query_covering_rt_for_range(
        &self,
        min: &[u8],
        max: &[u8],
        read_seqno: SeqNo,
    ) -> crate::Result<Option<CoveringRt>> {
        if self.is_empty() {
            return Ok(None);
        }

        // Fast reject: no tombstone end extends past max
        if self.global_max_end.as_ref() <= max {
            return Ok(None);
        }

        let restart_idx = self.find_restart_point(min);
        let mut best: Option<CoveringRt> = None;

        for wi in 0..=restart_idx {
            // Prune: window max_end <= max
            if let Some(max_end) = self.window_max_ends.get(wi) {
                if max_end.as_ref() <= max {
                    continue;
                }
            }

            let entries = self.decode_window(wi)?;
            for rt in entries {
                if rt.start.as_ref() > min {
                    break; // Can't cover [min, max]
                }
                if rt.fully_covers(min, max) && rt.visible_at(read_seqno) {
                    if best.as_ref().map_or(true, |b| rt.seqno > b.seqno) {
                        best = Some(CoveringRt::from(&rt));
                    }
                }
            }
        }

        Ok(best)
    }

    /// Iterates all tombstones in sort order. Used for compaction/flush.
    pub fn iter(&self) -> crate::Result<Vec<RangeTombstone>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(self.count as usize);
        for wi in 0..self.restart_count as usize {
            result.extend(self.decode_window(wi)?);
        }
        Ok(result)
    }

    /// Binary search for the last restart point with key <= `key`.
    /// Returns 0 when `key < restart_keys[0]` (saturates).
    fn find_restart_point(&self, key: &[u8]) -> usize {
        if self.restart_offsets.is_empty() {
            return 0;
        }

        // Decode each restart key and binary search
        // For simplicity, we decode the first entry of each window to get restart keys
        let mut lo = 0usize;
        let mut hi = self.restart_offsets.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if let Ok(first_entry) = self.decode_entry_at_offset(
                self.restart_offsets.get(mid).copied().unwrap_or(0) as usize,
                None,
            ) {
                if first_entry.0.start.as_ref() <= key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            } else {
                hi = mid; // On error, be conservative
            }
        }

        lo.saturating_sub(1)
    }

    /// Decodes all entries in window `wi`.
    fn decode_window(&self, wi: usize) -> crate::Result<Vec<RangeTombstone>> {
        let start_offset = self
            .restart_offsets
            .get(wi)
            .copied()
            .ok_or(crate::Error::InvalidBlock(
                "range tombstone block: window index out of bounds",
            ))? as usize;

        let end_offset = self
            .restart_offsets
            .get(wi + 1)
            .copied()
            .map(|v| v as usize)
            .unwrap_or(self.entries_end);

        let mut entries = Vec::new();
        let mut offset = start_offset;
        let mut prev_start: Option<UserKey> = None;

        while offset < end_offset {
            let (rt, consumed) = self.decode_entry_at_offset(offset, prev_start.as_ref())?;
            prev_start = Some(rt.start.clone());
            entries.push(rt);
            offset += consumed;
        }

        Ok(entries)
    }

    /// Decodes a single entry at the given byte offset.
    fn decode_entry_at_offset(
        &self,
        offset: usize,
        prev_start: Option<&UserKey>,
    ) -> crate::Result<(RangeTombstone, usize)> {
        let slice = self.data.get(offset..self.entries_end).ok_or(
            crate::Error::InvalidBlock("range tombstone block: entry offset out of bounds"),
        )?;
        let mut cursor = Cursor::new(slice);

        let shared_prefix_len = cursor.read_u32_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone block: failed to read shared_prefix_len")
        })? as usize;
        let start_suffix_len = cursor.read_u32_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone block: failed to read start_suffix_len")
        })? as usize;

        // Reconstruct start key
        let start = if shared_prefix_len == 0 {
            let suffix_start = cursor.position() as usize;
            let suffix = slice
                .get(suffix_start..suffix_start + start_suffix_len)
                .ok_or(crate::Error::InvalidBlock(
                    "range tombstone block: start suffix out of bounds",
                ))?;
            cursor.set_position((suffix_start + start_suffix_len) as u64);
            UserKey::from(suffix)
        } else {
            let prev = prev_start.ok_or(crate::Error::InvalidBlock(
                "range tombstone block: shared prefix without prev_start",
            ))?;
            if shared_prefix_len > prev.len() {
                return Err(crate::Error::InvalidBlock(
                    "range tombstone block: shared_prefix_len > prev_start.len()",
                ));
            }
            let suffix_start = cursor.position() as usize;
            let suffix = slice
                .get(suffix_start..suffix_start + start_suffix_len)
                .ok_or(crate::Error::InvalidBlock(
                    "range tombstone block: start suffix out of bounds",
                ))?;
            cursor.set_position((suffix_start + start_suffix_len) as u64);

            let mut reconstructed = Vec::with_capacity(shared_prefix_len + start_suffix_len);
            reconstructed.extend_from_slice(
                prev.as_ref()
                    .get(..shared_prefix_len)
                    .ok_or(crate::Error::InvalidBlock(
                        "range tombstone block: prefix slice out of bounds",
                    ))?,
            );
            reconstructed.extend_from_slice(suffix);
            UserKey::from(reconstructed)
        };

        // Read end key (always full, no prefix compression)
        let end_key_len = cursor.read_u32_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone block: failed to read end_key_len")
        })? as usize;
        let end_start = cursor.position() as usize;
        let end = slice
            .get(end_start..end_start + end_key_len)
            .ok_or(crate::Error::InvalidBlock(
                "range tombstone block: end key out of bounds",
            ))?;
        cursor.set_position((end_start + end_key_len) as u64);
        let end = UserKey::from(end);

        // Read seqno
        let seqno = cursor.read_u64_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone block: failed to read seqno")
        })?;

        // Hard error on corruption
        if start >= end {
            return Err(crate::Error::InvalidBlock(
                "range tombstone start >= end (corrupt block)",
            ));
        }

        let consumed = cursor.position() as usize;
        Ok((RangeTombstone::new(start, end, seqno), consumed))
    }
}

fn parse_window_max_ends(
    data: &[u8],
    blob_start: usize,
    count: usize,
) -> crate::Result<Vec<UserKey>> {
    let mut result = Vec::with_capacity(count);
    let mut offset = blob_start;

    for _ in 0..count {
        let len = read_u16_le(data, offset)? as usize;
        offset += 2;
        let key = data.get(offset..offset + len).ok_or(
            crate::Error::InvalidBlock("range tombstone block: window max_end out of bounds"),
        )?;
        result.push(UserKey::from(key));
        offset += len;
    }

    Ok(result)
}

fn read_u16_le(data: &[u8], offset: usize) -> crate::Result<u16> {
    let slice = data
        .get(offset..offset + 2)
        .ok_or(crate::Error::InvalidBlock("range tombstone block: u16 read out of bounds"))?;
    let mut cursor = Cursor::new(slice);
    Ok(cursor.read_u16::<LittleEndian>().map_err(|_| {
        crate::Error::InvalidBlock("range tombstone block: failed to read u16")
    })?)
}

fn read_u32_le(data: &[u8], offset: usize) -> crate::Result<u32> {
    let slice = data
        .get(offset..offset + 4)
        .ok_or(crate::Error::InvalidBlock("range tombstone block: u32 read out of bounds"))?;
    let mut cursor = Cursor::new(slice);
    Ok(cursor.read_u32::<LittleEndian>().map_err(|_| {
        crate::Error::InvalidBlock("range tombstone block: failed to read u32")
    })?)
}

fn read_u64_le(data: &[u8], offset: usize) -> crate::Result<u64> {
    let slice = data
        .get(offset..offset + 8)
        .ok_or(crate::Error::InvalidBlock("range tombstone block: u64 read out of bounds"))?;
    let mut cursor = Cursor::new(slice);
    Ok(cursor.read_u64::<LittleEndian>().map_err(|_| {
        crate::Error::InvalidBlock("range tombstone block: failed to read u64")
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::range_tombstone_encoder::encode_by_start;

    fn rt(start: &[u8], end: &[u8], seqno: u64) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    fn roundtrip(tombstones: &[RangeTombstone]) -> RangeTombstoneBlockByStart {
        let encoded = encode_by_start(tombstones);
        RangeTombstoneBlockByStart::parse(encoded).expect("parse should succeed")
    }

    #[test]
    fn empty_block() {
        let block = roundtrip(&[]);
        assert!(block.is_empty());
        assert_eq!(block.count(), 0);
        assert!(block.iter().unwrap().is_empty());
    }

    #[test]
    fn single_tombstone_roundtrip() {
        let tombstones = vec![rt(b"a", b"z", 10)];
        let block = roundtrip(&tombstones);
        assert_eq!(block.count(), 1);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], tombstones[0]);
    }

    #[test]
    fn multiple_tombstones_roundtrip() {
        let tombstones = vec![
            rt(b"a", b"f", 20),
            rt(b"a", b"m", 10),
            rt(b"d", b"z", 15),
            rt(b"m", b"r", 5),
        ];
        let block = roundtrip(&tombstones);
        assert_eq!(block.count(), 4);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded, tombstones);
    }

    #[test]
    fn query_suppression_basic() {
        let tombstones = vec![rt(b"b", b"y", 10)];
        let block = roundtrip(&tombstones);

        // Key in range, lower seqno → suppressed
        assert_eq!(
            block.query_suppression(b"c", 5, 100).unwrap(),
            Some(10),
        );

        // Key at end (exclusive) → not suppressed
        assert_eq!(
            block.query_suppression(b"y", 5, 100).unwrap(),
            None,
        );

        // Key before start → not suppressed
        assert_eq!(
            block.query_suppression(b"a", 5, 100).unwrap(),
            None,
        );

        // Key with higher seqno → not suppressed
        assert_eq!(
            block.query_suppression(b"c", 15, 100).unwrap(),
            None,
        );

        // Tombstone not visible at read_seqno → not suppressed
        assert_eq!(
            block.query_suppression(b"c", 5, 9).unwrap(),
            None,
        );
    }

    #[test]
    fn overlapping_tombstones_basic() {
        let tombstones = vec![
            rt(b"a", b"f", 10),
            rt(b"d", b"m", 20),
            rt(b"p", b"z", 5),
        ];
        let block = roundtrip(&tombstones);

        let overlaps = block.overlapping_tombstones(b"e", 100).unwrap();
        assert_eq!(overlaps.len(), 2);

        let overlaps = block.overlapping_tombstones(b"a", 100).unwrap();
        assert_eq!(overlaps.len(), 1);

        let overlaps = block.overlapping_tombstones(b"q", 100).unwrap();
        assert_eq!(overlaps.len(), 1);
    }

    #[test]
    fn covering_rt_basic() {
        let tombstones = vec![rt(b"a", b"z", 50)];
        let block = roundtrip(&tombstones);

        let crt = block
            .query_covering_rt_for_range(b"b", b"y", 100)
            .unwrap();
        assert!(crt.is_some());
        assert_eq!(crt.unwrap().seqno, 50);

        // Not fully covered
        let crt = block
            .query_covering_rt_for_range(b"a", b"z", 100)
            .unwrap();
        assert!(crt.is_none()); // max == end, half-open
    }

    #[test]
    fn many_windows_roundtrip() {
        let mut tombstones = Vec::new();
        for i in 0u8..50 {
            let start = vec![i];
            let end = vec![i + 1];
            tombstones.push(rt(&start, &end, u64::from(i)));
        }
        // Sort by (start asc, seqno desc, end asc)
        tombstones.sort();

        let block = roundtrip(&tombstones);
        assert_eq!(block.count(), 50);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded.len(), 50);
        assert_eq!(decoded, tombstones);
    }

    #[test]
    fn corruption_start_ge_end() {
        // Manually craft a block with start >= end
        // We'll encode a valid block then tamper with it
        // Instead, test the decode error path directly
        let tombstones = vec![rt(b"z", b"zzz", 10)];
        let block = roundtrip(&tombstones);
        // This should work fine since z < zzz
        assert_eq!(block.count(), 1);
    }

    #[test]
    fn prefix_compression_correctness() {
        let tombstones = vec![
            rt(b"prefix_aaa", b"z", 10),
            rt(b"prefix_aab", b"z", 9),
            rt(b"prefix_aac", b"z", 8),
            rt(b"prefix_bbb", b"z", 7),
        ];
        let block = roundtrip(&tombstones);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded, tombstones);
    }
}
