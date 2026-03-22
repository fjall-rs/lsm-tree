// Integration test: block-level encryption at rest roundtrip
//
// Verifies that data written with encryption enabled can be read back correctly
// through the full Tree API (write → flush → read).

#[cfg(feature = "encryption")]
mod encrypted {
    use lsm_tree::{AbstractTree, Aes256GcmProvider, Config, SequenceNumberCounter};
    use std::sync::Arc;

    fn test_key() -> [u8; 32] {
        [0x42; 32]
    }

    fn make_config(dir: &std::path::Path) -> Config {
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
    }

    #[test]
    fn tree_write_flush_read_encrypted() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let encryption = Arc::new(Aes256GcmProvider::new(&test_key()));

        let tree = make_config(dir.path())
            .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
                lsm_tree::CompressionType::None,
            ))
            .with_encryption(Some(encryption))
            .open()?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        tree.flush_active_memtable(0)?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .expect("key should exist");
            assert_eq!(got.as_ref(), expected.as_bytes(), "mismatch at key {key}");
        }

        assert!(tree.get(b"nonexistent", lsm_tree::MAX_SEQNO)?.is_none());
        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn tree_write_flush_read_encrypted_with_lz4() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let encryption = Arc::new(Aes256GcmProvider::new(&test_key()));

        let tree = make_config(dir.path())
            .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
                lsm_tree::CompressionType::Lz4,
            ))
            .with_encryption(Some(encryption))
            .open()?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let val = format!("value-{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), i.into());
        }

        tree.flush_active_memtable(0)?;

        for i in 0u32..100 {
            let key = format!("key-{i:05}");
            let expected = format!("value-{i:05}");
            let got = tree
                .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                .expect("key should exist");
            assert_eq!(got.as_ref(), expected.as_bytes(), "mismatch at key {key}");
        }

        Ok(())
    }

    #[test]
    fn encrypted_data_differs_on_disk() -> lsm_tree::Result<()> {
        let dir_plain = tempfile::tempdir()?;
        let dir_encrypted = tempfile::tempdir()?;

        let encryption = Arc::new(Aes256GcmProvider::new(&test_key()));

        let tree_plain = make_config(dir_plain.path())
            .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
                lsm_tree::CompressionType::None,
            ))
            .open()?;

        let tree_enc = make_config(dir_encrypted.path())
            .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
                lsm_tree::CompressionType::None,
            ))
            .with_encryption(Some(encryption))
            .open()?;

        let data = b"this is sensitive data that should be encrypted on disk";
        tree_plain.insert(b"secret", &data[..], 0);
        tree_enc.insert(b"secret", &data[..], 0);

        tree_plain.flush_active_memtable(0)?;
        tree_enc.flush_active_memtable(0)?;

        // Read back from encrypted tree to verify correctness
        let got = tree_enc
            .get(b"secret", lsm_tree::MAX_SEQNO)?
            .expect("should exist");
        assert_eq!(got.as_ref(), data);

        // Compare SST files on disk — they should differ
        let plain_sst = find_table_file(dir_plain.path());
        let enc_sst = find_table_file(dir_encrypted.path());

        let plain_bytes = std::fs::read(&plain_sst)?;
        let enc_bytes = std::fs::read(&enc_sst)?;

        assert_ne!(
            plain_bytes, enc_bytes,
            "encrypted SST should differ from plaintext SST"
        );

        // The plaintext SST should contain the raw value bytes
        assert!(
            contains_bytes(&plain_bytes, data),
            "plaintext SST should contain raw value"
        );

        // The encrypted SST should NOT contain the raw value bytes
        assert!(
            !contains_bytes(&enc_bytes, data),
            "encrypted SST must not contain raw value"
        );

        Ok(())
    }

    #[test]
    fn encrypted_data_tamper_fails() -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let encryption = Arc::new(Aes256GcmProvider::new(&test_key()));

        // Write and flush encrypted data
        {
            let tree = make_config(dir.path())
                .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
                    lsm_tree::CompressionType::None,
                ))
                .with_encryption(Some(encryption.clone()))
                .open()?;

            tree.insert(b"secret", b"sensitive-value", 0);
            tree.flush_active_memtable(0)?;
        }
        // Tree dropped — all files flushed

        // Tamper with the SST file on disk
        let sst_path = find_table_file(dir.path());
        let mut sst_bytes = std::fs::read(&sst_path)?;

        // Flip bytes in the middle of the data section (after the header)
        let tamper_offset = sst_bytes.len() / 2;
        for i in 0..8 {
            if tamper_offset + i < sst_bytes.len() {
                #[expect(clippy::indexing_slicing, reason = "bounds checked")]
                {
                    sst_bytes[tamper_offset + i] ^= 0xFF;
                }
            }
        }
        std::fs::write(&sst_path, &sst_bytes)?;

        // Reopen the tree and attempt to read — should fail with
        // checksum mismatch or decryption error, either during open
        // (table recovery reads meta/index blocks) or during get().
        let open_result = make_config(dir.path())
            .data_block_compression_policy(lsm_tree::config::CompressionPolicy::all(
                lsm_tree::CompressionType::None,
            ))
            .with_encryption(Some(encryption))
            .open();

        match open_result {
            Err(_) => {
                // Tamper detected during recovery — expected
            }
            Ok(tree) => {
                // Recovery succeeded (tamper was in data block, not meta/index).
                // The read must fail.
                let result = tree.get(b"secret", lsm_tree::MAX_SEQNO);
                assert!(
                    result.is_err(),
                    "reading tampered encrypted data should fail, got: {result:?}"
                );
            }
        }

        Ok(())
    }

    fn find_table_file(dir: &std::path::Path) -> std::path::PathBuf {
        // Table files live in the `tables/` subdirectory, named by numeric ID
        let tables_dir = dir.join("tables");
        let search_dir = if tables_dir.exists() {
            &tables_dir
        } else {
            dir
        };
        for entry in std::fs::read_dir(search_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                return entry.path();
            }
        }
        panic!("no table file found in {}", search_dir.display());
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
