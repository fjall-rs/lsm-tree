use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter};

#[test]
fn point_reads_preserve_versions_and_deletions_across_tables() -> lsm_tree::Result<()> {
    for weak in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        let keys: Vec<_> = (0..64)
            .map(|i| format!("{}:{i:04}", "prefix".repeat(32)))
            .collect();
        for key in &keys {
            tree.insert(key.as_bytes(), b"old", 10);
        }
        tree.flush_active_memtable(0)?;
        for key in &keys {
            if weak {
                tree.remove_weak(key.as_bytes(), 20);
            } else {
                tree.remove(key.as_bytes(), 20);
            }
        }
        // Also check the active-memtable deletion before it becomes a table.
        assert!(tree.get(keys[0].as_bytes(), SeqNo::MAX)?.is_none());
        tree.flush_active_memtable(0)?;
        for key in &keys {
            assert!(tree.get(key.as_bytes(), 10)?.is_none());
            assert_eq!(tree.get(key.as_bytes(), 20)?.unwrap().as_ref(), b"old");
            assert!(tree.get(key.as_bytes(), 21)?.is_none());
            let full = tree.get_internal_entry(key.as_bytes(), 20)?.unwrap();
            assert_eq!(full.key.user_key.as_ref(), key.as_bytes());
            assert_eq!(full.key.seqno, 10);
            assert_eq!(full.value.as_ref(), b"old");
        }
        for key in &keys {
            tree.insert(key.as_bytes(), b"", 30);
        }
        assert_eq!(tree.get(keys[0].as_bytes(), 31)?.unwrap().len(), 0);
        tree.flush_active_memtable(0)?;
        for key in &keys {
            assert!(tree.get(key.as_bytes(), 30)?.is_none());
            assert_eq!(tree.get(key.as_bytes(), 31)?.unwrap().len(), 0);
        }
        assert!(tree.get(b"missing", SeqNo::MAX)?.is_none());
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), keys.len());
    }
    Ok(())
}

#[test]
fn blob_point_reads_resolve_with_the_query_key() -> lsm_tree::Result<()> {
    for capacity in [0, 1024 * 1024] {
        let folder = tempfile::tempdir()?;
        let tree = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(
            capacity,
        )))
        .with_kv_separation(Some(
            lsm_tree::KvSeparationOptions::default().separation_threshold(64),
        ))
        .open()?;
        let keys: Vec<_> = (0..32)
            .map(|i| format!("{}:{i:04}", "shared".repeat(32)))
            .collect();
        for (i, key) in keys.iter().enumerate() {
            tree.insert(
                key.as_bytes(),
                vec![i as u8; if i % 2 == 0 { 8 } else { 256 }],
                1,
            );
        }
        tree.flush_active_memtable(0)?;
        assert!(tree.blob_file_count() > 0);
        for _ in 0..2 {
            for (i, key) in keys.iter().enumerate() {
                let expected = vec![i as u8; if i % 2 == 0 { 8 } else { 256 }];
                assert_eq!(tree.get(key.as_bytes(), 2)?.unwrap().as_ref(), expected);
                assert_eq!(
                    tree.size_of(key.as_bytes(), 2)?,
                    Some(expected.len() as u32)
                );
                assert!(tree.get(key.as_bytes(), 1)?.is_none());
            }
        }
        tree.remove(keys[1].as_bytes(), 2);
        tree.flush_active_memtable(0)?;
        assert!(tree.get(keys[1].as_bytes(), 3)?.is_none());
        assert_eq!(tree.size_of(keys[1].as_bytes(), 3)?, None);
        assert_eq!(tree.get(keys[1].as_bytes(), 2)?.unwrap().len(), 256);
        assert_eq!(tree.iter(SeqNo::MAX, None).count(), keys.len() - 1);
    }
    Ok(())
}
