// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Range tombstone block sorted by `(end desc, seqno desc)`.
//!
//! Used for reverse iteration only. No `seek_by_end` — reverse iteration
//! starts from the beginning of the block (largest ends first). See Part 4.4
//! of the design doc for rationale.

use crate::range_tombstone::RangeTombstone;
use crate::{SeqNo, UserKey};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use varint_rs::VarintReader;

/// Parsed range tombstone block in ByEndDesc layout.
pub struct RangeTombstoneBlockByEndDesc {
    data: Vec<u8>,
    count: u32,
    restart_count: u32,
    restart_offsets: Vec<u32>,
    global_max_end: UserKey,
    max_seqno: u64,
    entries_end: usize,
}

impl RangeTombstoneBlockByEndDesc {
    /// Parses the backward-parseable footer.
    pub fn parse(data: Vec<u8>) -> crate::Result<Self> {
        if data.len() < 8 {
            return Err(crate::Error::InvalidBlock(
                "range tombstone by-end block too small",
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

        // Step 3: max_seqno
        let max_seqno_pos = restart_array_start - 8;
        let max_seqno = read_u64_le(&data, max_seqno_pos)?;

        // Step 4: max_end_len
        let max_end_len_pos = max_seqno_pos - 2;
        let max_end_len = read_u16_le(&data, max_end_len_pos)? as usize;

        // Step 5: global_max_end
        let gme_start = max_end_len_pos - max_end_len;
        let global_max_end = data
            .get(gme_start..gme_start + max_end_len)
            .ok_or(crate::Error::InvalidBlock(
                "range tombstone by-end: global_max_end out of bounds",
            ))?;
        let global_max_end = UserKey::from(global_max_end);

        // entries_end = start of global_max_end blob
        let entries_end = gme_start;

        Ok(Self {
            data,
            count,
            restart_count,
            restart_offsets,
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

    /// Returns the global max end key (fast-reject metadata).
    pub fn global_max_end(&self) -> &UserKey {
        &self.global_max_end
    }

    /// Returns the max seqno in the block.
    pub fn max_seqno(&self) -> u64 {
        self.max_seqno
    }

    /// Iterates all tombstones in `(end desc, seqno desc)` order from the beginning.
    ///
    /// For reverse iteration, tombstones are streamed from largest `end` to smallest.
    /// Tombstones with `end > current_key` are activated immediately during reverse scan init.
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

    /// Decodes all entries in window `wi`.
    fn decode_window(&self, wi: usize) -> crate::Result<Vec<RangeTombstone>> {
        let start_offset = self
            .restart_offsets
            .get(wi)
            .copied()
            .ok_or(crate::Error::InvalidBlock(
                "range tombstone by-end: window index out of bounds",
            ))? as usize;

        let end_offset = self
            .restart_offsets
            .get(wi + 1)
            .copied()
            .map(|v| v as usize)
            .unwrap_or(self.entries_end);

        let mut entries = Vec::new();
        let mut offset = start_offset;
        let mut prev_end: Option<UserKey> = None;

        while offset < end_offset {
            let (rt, consumed) = self.decode_entry_at_offset(offset, prev_end.as_ref())?;
            prev_end = Some(rt.end.clone());
            entries.push(rt);
            offset += consumed;
        }

        Ok(entries)
    }

    /// Decodes a single entry. ByEndDesc prefix-compresses `end` keys.
    fn decode_entry_at_offset(
        &self,
        offset: usize,
        prev_end: Option<&UserKey>,
    ) -> crate::Result<(RangeTombstone, usize)> {
        let slice = self.data.get(offset..self.entries_end).ok_or(
            crate::Error::InvalidBlock("range tombstone by-end: entry offset out of bounds"),
        )?;
        let mut cursor = Cursor::new(slice);

        // Read shared_prefix_len (for END key prefix compression)
        let shared_prefix_len = cursor.read_u32_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone by-end: failed to read shared_prefix_len")
        })? as usize;
        let end_suffix_len = cursor.read_u32_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone by-end: failed to read end_suffix_len")
        })? as usize;

        // Reconstruct end key
        let end = if shared_prefix_len == 0 {
            let suffix_start = cursor.position() as usize;
            let suffix = slice
                .get(suffix_start..suffix_start + end_suffix_len)
                .ok_or(crate::Error::InvalidBlock(
                    "range tombstone by-end: end suffix out of bounds",
                ))?;
            cursor.set_position((suffix_start + end_suffix_len) as u64);
            UserKey::from(suffix)
        } else {
            let prev = prev_end.ok_or(crate::Error::InvalidBlock(
                "range tombstone by-end: shared prefix without prev_end",
            ))?;
            if shared_prefix_len > prev.len() {
                return Err(crate::Error::InvalidBlock(
                    "range tombstone by-end: shared_prefix_len > prev_end.len()",
                ));
            }
            let suffix_start = cursor.position() as usize;
            let suffix = slice
                .get(suffix_start..suffix_start + end_suffix_len)
                .ok_or(crate::Error::InvalidBlock(
                    "range tombstone by-end: end suffix out of bounds",
                ))?;
            cursor.set_position((suffix_start + end_suffix_len) as u64);

            let mut reconstructed = Vec::with_capacity(shared_prefix_len + end_suffix_len);
            reconstructed.extend_from_slice(
                prev.as_ref()
                    .get(..shared_prefix_len)
                    .ok_or(crate::Error::InvalidBlock(
                        "range tombstone by-end: prefix slice out of bounds",
                    ))?,
            );
            reconstructed.extend_from_slice(suffix);
            UserKey::from(reconstructed)
        };

        // Read start key (always full, no prefix compression)
        let start_key_len = cursor.read_u32_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone by-end: failed to read start_key_len")
        })? as usize;
        let start_start = cursor.position() as usize;
        let start = slice
            .get(start_start..start_start + start_key_len)
            .ok_or(crate::Error::InvalidBlock(
                "range tombstone by-end: start key out of bounds",
            ))?;
        cursor.set_position((start_start + start_key_len) as u64);
        let start = UserKey::from(start);

        // Read seqno
        let seqno = cursor.read_u64_varint().map_err(|_| {
            crate::Error::InvalidBlock("range tombstone by-end: failed to read seqno")
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

fn read_u16_le(data: &[u8], offset: usize) -> crate::Result<u16> {
    let slice = data
        .get(offset..offset + 2)
        .ok_or(crate::Error::InvalidBlock("range tombstone by-end: u16 read out of bounds"))?;
    let mut cursor = Cursor::new(slice);
    Ok(cursor.read_u16::<LittleEndian>().map_err(|_| {
        crate::Error::InvalidBlock("range tombstone by-end: failed to read u16")
    })?)
}

fn read_u32_le(data: &[u8], offset: usize) -> crate::Result<u32> {
    let slice = data
        .get(offset..offset + 4)
        .ok_or(crate::Error::InvalidBlock("range tombstone by-end: u32 read out of bounds"))?;
    let mut cursor = Cursor::new(slice);
    Ok(cursor.read_u32::<LittleEndian>().map_err(|_| {
        crate::Error::InvalidBlock("range tombstone by-end: failed to read u32")
    })?)
}

fn read_u64_le(data: &[u8], offset: usize) -> crate::Result<u64> {
    let slice = data
        .get(offset..offset + 8)
        .ok_or(crate::Error::InvalidBlock("range tombstone by-end: u64 read out of bounds"))?;
    let mut cursor = Cursor::new(slice);
    Ok(cursor.read_u64::<LittleEndian>().map_err(|_| {
        crate::Error::InvalidBlock("range tombstone by-end: failed to read u64")
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::range_tombstone_encoder::encode_by_end_desc;

    fn rt(start: &[u8], end: &[u8], seqno: u64) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    fn roundtrip(tombstones: &[RangeTombstone]) -> RangeTombstoneBlockByEndDesc {
        let encoded = encode_by_end_desc(tombstones);
        RangeTombstoneBlockByEndDesc::parse(encoded).expect("parse should succeed")
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
    fn multiple_tombstones_end_desc_order() {
        // Input must be sorted by (end desc, seqno desc)
        let tombstones = vec![
            rt(b"a", b"z", 20),
            rt(b"b", b"z", 10),
            rt(b"c", b"m", 15),
            rt(b"d", b"f", 5),
        ];
        let block = roundtrip(&tombstones);
        assert_eq!(block.count(), 4);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded, tombstones);
    }

    #[test]
    fn global_max_end_and_max_seqno() {
        let tombstones = vec![
            rt(b"a", b"zzz", 20),
            rt(b"b", b"mmm", 30),
        ];
        let block = roundtrip(&tombstones);
        assert_eq!(block.global_max_end().as_ref(), b"zzz");
        assert_eq!(block.max_seqno(), 30);
    }

    #[test]
    fn many_windows_roundtrip() {
        // More than RESTART_INTERVAL entries, sorted by end desc
        let mut tombstones = Vec::new();
        for i in (0u8..50).rev() {
            let start = vec![b'a'];
            let end = vec![b'z', 50 - i]; // ensure start < end
            tombstones.push(rt(&start, &end, u64::from(i)));
        }

        let block = roundtrip(&tombstones);
        assert_eq!(block.count(), 50);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded.len(), 50);
        assert_eq!(decoded, tombstones);
    }

    #[test]
    fn prefix_compression_on_end_keys() {
        let tombstones = vec![
            rt(b"a", b"prefix_zzz", 10),
            rt(b"b", b"prefix_yyy", 9),
            rt(b"c", b"prefix_xxx", 8),
        ];
        let block = roundtrip(&tombstones);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded, tombstones);
    }

    #[test]
    fn deterministic_tiebreaker_start_asc() {
        // Test that entries with same (end, seqno) but different start
        // are preserved in input order (which should include start asc tiebreaker)
        let tombstones = vec![
            rt(b"a", b"z", 10),
            rt(b"b", b"z", 10),
            rt(b"c", b"z", 10),
        ];
        let block = roundtrip(&tombstones);
        let decoded = block.iter().unwrap();
        assert_eq!(decoded, tombstones);
    }
}
