// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#![cfg(feature = "tool")]

//! Integration tests for the `lsm` CLI tool binary.
//!
//! These tests run the actual binary and verify its behavior.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Get the path to the lsm binary
fn lsm_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name
    path.pop(); // remove deps
    path.push("lsm");
    path
}

/// Run the lsm binary with flags and CLI arguments
fn run_cli(db_path: &std::path::Path, flags: &[&str], args: &[&str]) -> (String, String, bool) {
    let mut cmd_args = flags.to_vec();
    cmd_args.push(db_path.to_str().unwrap());
    cmd_args.extend(args);

    let output = Command::new(lsm_binary())
        .args(&cmd_args)
        .output()
        .expect("Failed to execute lsm binary");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr, output.status.success())
}

/// Run the lsm binary in shell mode with piped input
fn run_shell(db_path: &std::path::Path, input: &str) -> (String, String, bool) {
    let mut child = Command::new(lsm_binary())
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn lsm binary");

    {
        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        stdin
            .write_all(input.as_bytes())
            .expect("Failed to write to stdin");
    }

    let output = child.wait_with_output().expect("Failed to read output");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr, output.status.success())
}

/// Create a temporary directory for test databases
fn temp_db() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}

// ============================================================================
// CLI Command Tests
// ============================================================================

#[test]
fn test_cli_set_and_get() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Set a value
    let (stdout, stderr, success) = run_cli(&db_path, &[], &["set", "mykey", "myvalue"]);
    assert!(success, "set failed: {}", stderr);
    assert!(stdout.contains("OK"), "Expected OK in output: {}", stdout);

    // Get the value back
    let (stdout, stderr, success) = run_cli(&db_path, &[], &["get", "mykey"]);
    assert!(success, "get failed: {}", stderr);
    assert!(
        stdout.trim() == "myvalue",
        "Expected 'myvalue', got: {}",
        stdout
    );
}

#[test]
fn test_cli_get_nonexistent() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_cli(&db_path, &[], &["get", "nonexistent"]);
    assert!(success);
    assert!(
        stdout.contains("(not found)"),
        "Expected '(not found)': {}",
        stdout
    );
}

#[test]
fn test_cli_del() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Set then delete
    run_cli(&db_path, &[], &["set", "key1", "value1"]);
    let (stdout, stderr, success) = run_cli(&db_path, &[], &["del", "key1"]);
    assert!(success, "del failed: {}", stderr);
    assert!(stdout.contains("OK"), "Expected OK: {}", stdout);

    // Verify deleted
    let (stdout, _, _) = run_cli(&db_path, &[], &["get", "key1"]);
    assert!(
        stdout.contains("(not found)"),
        "Key should be deleted: {}",
        stdout
    );
}

#[test]
fn test_cli_scan() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Add some data
    run_cli(&db_path, &[], &["set", "apple", "red"]);
    run_cli(&db_path, &[], &["set", "banana", "yellow"]);
    run_cli(&db_path, &[], &["set", "cherry", "red"]);

    // Scan all
    let (stdout, _, success) = run_cli(&db_path, &[], &["scan"]);
    assert!(success);
    assert!(stdout.contains("apple = red"));
    assert!(stdout.contains("banana = yellow"));
    assert!(stdout.contains("cherry = red"));
    assert!(stdout.contains("(3 items)"));
}

#[test]
fn test_cli_scan_with_prefix() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    run_cli(&db_path, &[], &["set", "user:1", "alice"]);
    run_cli(&db_path, &[], &["set", "user:2", "bob"]);
    run_cli(&db_path, &[], &["set", "item:1", "widget"]);

    // Scan with prefix
    let (stdout, _, success) = run_cli(&db_path, &[], &["scan", "user:"]);
    assert!(success);
    assert!(stdout.contains("user:1 = alice"));
    assert!(stdout.contains("user:2 = bob"));
    assert!(!stdout.contains("item:1"));
    assert!(stdout.contains("(2 items)"));
}

#[test]
fn test_cli_scan_aliases() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    run_cli(&db_path, &[], &["set", "key1", "val1"]);

    // Test 'list' alias
    let (stdout1, _, success1) = run_cli(&db_path, &[], &["list"]);
    assert!(success1);
    assert!(stdout1.contains("key1 = val1"));

    // Test 'ls' alias
    let (stdout2, _, success2) = run_cli(&db_path, &[], &["ls"]);
    assert!(success2);
    assert!(stdout2.contains("key1 = val1"));
}

#[test]
fn test_cli_range() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    run_cli(&db_path, &[], &["set", "a", "1"]);
    run_cli(&db_path, &[], &["set", "b", "2"]);
    run_cli(&db_path, &[], &["set", "c", "3"]);
    run_cli(&db_path, &[], &["set", "d", "4"]);

    // Range [b, d) should include b and c
    let (stdout, _, success) = run_cli(&db_path, &[], &["range", "b", "d"]);
    assert!(success);
    assert!(stdout.contains("b = 2"));
    assert!(stdout.contains("c = 3"));
    assert!(!stdout.contains("a = 1"));
    assert!(!stdout.contains("d = 4"));
    assert!(stdout.contains("(2 items)"));
}

#[test]
fn test_cli_count() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    run_cli(&db_path, &[], &["set", "k1", "v1"]);
    run_cli(&db_path, &[], &["set", "k2", "v2"]);
    run_cli(&db_path, &[], &["set", "k3", "v3"]);

    let (stdout, _, success) = run_cli(&db_path, &[], &["count"]);
    assert!(success);
    assert!(stdout.trim() == "3", "Expected 3, got: {}", stdout);
}

#[test]
fn test_cli_info() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    run_cli(&db_path, &[], &["set", "key", "value"]);

    let (stdout, _, success) = run_cli(&db_path, &[], &["info"]);
    assert!(success);
    assert!(stdout.contains("Path:"));
    assert!(stdout.contains("Tables:"));
    assert!(stdout.contains("Approximate items:"));
}

#[test]
fn test_cli_flush() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    run_cli(&db_path, &[], &["set", "key", "value"]);
    let (stdout, _, success) = run_cli(&db_path, &[], &["flush"]);
    assert!(success);
    assert!(
        stdout.contains("OK (flushed)"),
        "Expected flushed: {}",
        stdout
    );
}

// ============================================================================
// Shell Mode Tests
// ============================================================================

#[test]
fn test_shell_basic_commands() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, stderr, success) =
        run_shell(&db_path, "set foo bar\nget foo\nscan\ncount\nexit\n");

    assert!(success, "Shell failed: {}", stderr);
    assert!(stdout.contains("OK (set)"), "set failed: {}", stdout);
    assert!(stdout.contains("bar"), "get failed: {}", stdout);
    assert!(stdout.contains("foo = bar"), "scan failed: {}", stdout);
    assert!(stdout.contains("1"), "count failed: {}", stdout);
}

#[test]
fn test_shell_quoted_values() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set mykey \"hello world with spaces\"\nget mykey\nexit\n",
    );

    assert!(success);
    assert!(stdout.contains("hello world with spaces"));
}

#[test]
fn test_shell_single_quotes() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set mykey 'single quoted value'\nget mykey\nexit\n",
    );

    assert!(success);
    assert!(stdout.contains("single quoted value"));
}

#[test]
fn test_shell_exit_flushes() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Set value and exit (should flush)
    run_shell(&db_path, "set persistent_key persistent_value\nexit\n");

    // Reopen and verify data persisted
    let (stdout, _, success) = run_cli(&db_path, &[], &["get", "persistent_key"]);
    assert!(success);
    assert!(
        stdout.contains("persistent_value"),
        "Data should persist after exit: {}",
        stdout
    );
}

#[test]
fn test_shell_abort_no_flush() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Set value and abort (should NOT flush)
    run_shell(&db_path, "set temp_key temp_value\nabort\n");

    // Reopen and verify data did NOT persist
    let (stdout, _, _) = run_cli(&db_path, &[], &["get", "temp_key"]);
    assert!(
        stdout.contains("(not found)"),
        "Data should not persist after abort: {}",
        stdout
    );
}

#[test]
fn test_shell_quit_alias() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(&db_path, "set k v\nquit\n");
    assert!(success);
    assert!(stdout.contains("OK (flushed)"));
}

// ============================================================================
// Batch Operation Tests
// ============================================================================

#[test]
fn test_batch_begin_commit() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, stderr, success) = run_shell(
        &db_path,
        "begin\nset k1 v1\nset k2 v2\nset k3 v3\ndel k2\ncommit\nscan\nexit\n",
    );

    assert!(success, "Shell failed: {}", stderr);
    assert!(
        stdout.contains("OK (batch started)"),
        "begin failed: {}",
        stdout
    );
    assert!(
        stdout.contains("OK (batched, ready to commit)"),
        "batched set failed: {}",
        stdout
    );
    assert!(
        stdout.contains("OK (batch committed"),
        "commit failed: {}",
        stdout
    );
    assert!(stdout.contains("k1 = v1"));
    assert!(!stdout.contains("k2 = v2"));
    assert!(stdout.contains("k3 = v3"));
}

#[test]
fn test_batch_rollback() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set existing before\nbegin\nset k1 v1\nrollback\nscan\nexit\n",
    );

    assert!(success);
    assert!(stdout.contains("OK (batch rolled back)"));
    // k1 should not exist after rollback
    assert!(!stdout.contains("k1 = v1"));
    // existing key should still be there
    assert!(stdout.contains("existing = before"));
}

#[test]
fn test_batch_get_reads_from_batch() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "begin\nset newkey newvalue\nget newkey\nrollback\nexit\n",
    );

    assert!(success);
    // get should return the batched value
    assert!(
        stdout.contains("newvalue"),
        "Should read from batch: {}",
        stdout
    );
}

#[test]
fn test_batch_del_in_batch() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set mykey myvalue\nflush\nbegin\ndel mykey\nget mykey\nrollback\nexit\n",
    );

    assert!(success);
    assert!(
        stdout.contains("(deleted in batch)"),
        "Should show deleted in batch: {}",
        stdout
    );
}

#[test]
fn test_batch_info_shows_operations() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) =
        run_shell(&db_path, "begin\nset k1 v1\ndel k2\ninfo\nrollback\nexit\n");

    assert!(success);
    assert!(
        stdout.contains("Active batch (2 operations)"),
        "Should show batch info: {}",
        stdout
    );
    assert!(stdout.contains("SET k1 = v1"));
    assert!(stdout.contains("DEL k2"));
}

#[test]
fn test_batch_double_begin_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "begin\nbegin\nrollback\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("batch already active"),
        "Should error on double begin: {}",
        stderr
    );
}

#[test]
fn test_commit_without_batch_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "commit\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("no active batch"),
        "Should error: {}",
        stderr
    );
}

#[test]
fn test_rollback_without_batch_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "rollback\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("no active batch"),
        "Should error: {}",
        stderr
    );
}

#[test]
fn test_flush_warns_about_pending_batch() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "begin\nset k v\nflush\nrollback\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("WARNING: Active batch"),
        "Should warn about pending batch: {}",
        stderr
    );
}

#[test]
fn test_scan_warns_about_batch() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "begin\nset k v\nscan\nrollback\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("scan ignores uncommitted batch"),
        "Should warn: {}",
        stderr
    );
}

#[test]
fn test_range_warns_about_batch() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "begin\nset k v\nrange a z\nrollback\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("range ignores uncommitted batch"),
        "Should warn: {}",
        stderr
    );
}

#[test]
fn test_exit_warns_about_uncommitted_batch() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "begin\nset k v\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("discarding uncommitted batch"),
        "Should warn: {}",
        stderr
    );
}

// ============================================================================
// Scan Long/Verbose Mode Tests
// ============================================================================

#[test]
fn test_scan_long_flag() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(&db_path, "set k1 v1\nscan -l\nexit\n");

    assert!(success);
    assert!(
        stdout.contains("=== Active Memtable ==="),
        "Should show memtable section: {}",
        stdout
    );
    assert!(stdout.contains("seqno="), "Should show seqno: {}", stdout);
    assert!(
        stdout.contains("type=Value"),
        "Should show type: {}",
        stdout
    );
}

#[test]
fn test_scan_memtable_match_no_match() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(&db_path, "set k1 v1\nset x2 v2\nscan -l k\nexit\n");

    assert!(success);
    assert!(
        stdout.contains("=== Active Memtable ==="),
        "Should show memtable section: {}",
        stdout
    );
    assert!(
        stdout.contains("k1 = v1"),
        "Should show 'k1 = v1' as we scanned for k: {}",
        stdout
    );
    assert!(
        !stdout.contains("x2 = v2"),
        "Should not contain 'x2 = v2' as we scanned for k: {}",
        stdout
    );
}

#[test]
fn test_scan_long_shows_tombstones() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(&db_path, "set k1 v1\ndel k1\nscan -l\nexit\n");

    assert!(success);
    assert!(
        stdout.contains("type=Tombstone"),
        "Should show tombstone: {}",
        stdout
    );
}

#[test]
fn test_ll_alias_for_scan_long() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(&db_path, "set k1 v1\nll\nexit\n");

    assert!(success);
    assert!(
        stdout.contains("=== Active Memtable ==="),
        "ll should be scan long: {}",
        stdout
    );
}

// ============================================================================
// Data Persistence Tests
// ============================================================================

#[test]
fn test_data_persists_across_sessions() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // First session: write and flush
    run_shell(&db_path, "set persistent data\nflush\nabort\n");

    // Second session: read
    let (stdout, _, success) = run_cli(&db_path, &[], &["get", "persistent"]);
    assert!(success);
    assert!(stdout.contains("data"), "Data should persist: {}", stdout);
}

#[test]
fn test_unflushed_data_lost_on_abort() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Write without flush and abort
    run_shell(&db_path, "set unflushed value\nabort\n");

    // Verify lost
    let (stdout, _, _) = run_cli(&db_path, &[], &["get", "unflushed"]);
    assert!(
        stdout.contains("(not found)"),
        "Unflushed data should be lost: {}",
        stdout
    );
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_unclosed_quote_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "set key \"unclosed\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("unclosed quote"),
        "Should error on unclosed quote: {}",
        stderr
    );
}

#[test]
fn test_unknown_command_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "notacommand\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("error"),
        "Should error on unknown command: {}",
        stderr
    );
}

#[test]
fn test_missing_argument_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (_, stderr, success) = run_shell(&db_path, "set onlykey\nexit\n");

    assert!(success);
    assert!(
        stderr.contains("required") || stderr.contains("VALUE"),
        "Should error on missing argument: {}",
        stderr
    );
}

// ============================================================================
// Help Tests
// ============================================================================

#[test]
fn test_cli_help() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let output = Command::new(lsm_binary())
        .arg(db_path)
        .arg("--help")
        .output()
        .expect("Failed to run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("CLI tool for interacting with LSM trees"));
    assert!(stdout.contains("get"));
    assert!(stdout.contains("set"));
    assert!(stdout.contains("scan"));
}

#[test]
fn test_shell_help_command() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // The shell uses clap's built-in help, which outputs to stderr
    let (stdout, stderr, success) = run_shell(&db_path, "help\nexit\n");

    assert!(success);
    // Help output goes to stdout or stderr depending on clap version
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("Available Commands")
            || combined.contains("get")
            || combined.contains("Commands:"),
        "Help should show commands. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_empty_database_scan() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_cli(&db_path, &[], &["scan"]);
    assert!(success);
    assert!(stdout.contains("(0 items)"));
}

#[test]
fn test_empty_database_count() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_cli(&db_path, &[], &["count"]);
    assert!(success);
    assert!(stdout.trim() == "0");
}

#[test]
fn test_special_characters_in_key() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set \"key:with:colons\" value\nget \"key:with:colons\"\nexit\n",
    );

    assert!(success);
    assert!(stdout.contains("value"));
}

#[test]
fn test_special_characters_in_value() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set mykey \"value with = equals and : colons\"\nget mykey\nexit\n",
    );

    assert!(success);
    assert!(stdout.contains("value with = equals and : colons"));
}

#[test]
fn test_overwrite_key() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(
        &db_path,
        "set mykey original\nset mykey updated\nget mykey\nexit\n",
    );

    assert!(success);
    assert!(stdout.contains("updated"));
    // Should not contain 'original' in the get output (might be in set output)
    let lines: Vec<&str> = stdout.lines().collect();
    let get_output = lines.iter().find(|l| !l.contains("OK")).unwrap();
    assert!(
        get_output.contains("updated") && !get_output.contains("original"),
        "Should show updated value: {}",
        get_output
    );
}

#[test]
fn test_delete_nonexistent_key() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Deleting a non-existent key should succeed (creates a tombstone)
    let (stdout, _, success) = run_cli(&db_path, &[], &["del", "nonexistent"]);
    assert!(success);
    assert!(stdout.contains("OK"));
}

#[test]
fn test_compact_empty_database() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_cli(&db_path, &[], &["compact"]);
    assert!(success);
    assert!(stdout.contains("OK"));
}

#[test]
fn test_multiple_flush_calls() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, _, success) = run_shell(&db_path, "set k1 v1\nflush\nflush\nflush\nexit\n");

    assert!(success);
    // Multiple flushes should all succeed
    let flush_count = stdout.matches("OK (flushed)").count();
    assert!(
        flush_count >= 3,
        "Should have multiple flush OKs: {}",
        stdout
    );
}

#[test]
fn test_long_value_truncated_in_verbose_scan() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Create a value longer than 50 characters
    let long_value = "a".repeat(100);

    // Use scan -l (verbose/long mode) which truncates long values
    let (stdout, _, success) = run_shell(
        &db_path,
        &format!("set mykey {}\nscan -l\nexit\n", long_value),
    );

    assert!(success);
    // Value should be truncated with "..." in verbose scan output
    assert!(
        stdout.contains("..."),
        "Long value should be truncated with '...': {}",
        stdout
    );
    // Should show exactly 50 'a' characters followed by "..."
    assert!(
        stdout.contains(&format!("{}...", "a".repeat(50))),
        "Should show first 50 chars followed by '...': {}",
        stdout
    );
}

#[test]
fn test_long_value_truncated_in_batch_info() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Create a value longer than 50 characters
    let long_value = "x".repeat(75);

    let (stdout, _, success) = run_shell(
        &db_path,
        &format!("begin\nset batchkey {}\ninfo\nrollback\nexit\n", long_value),
    );

    assert!(success);
    // Value should be truncated with "..." in batch info output
    assert!(
        stdout.contains("..."),
        "Long value in batch info should be truncated with '...': {}",
        stdout
    );
    // Should show exactly 50 'x' characters followed by "..."
    assert!(
        stdout.contains(&format!("{}...", "x".repeat(50))),
        "Should show first 50 chars followed by '...': {}",
        stdout
    );
}

#[test]
fn test_shell_empty_lines() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Send empty lines interspersed with commands
    let (stdout, _, success) = run_shell(&db_path, "\n\nset key1 value1\n\n\nget key1\n\nexit\n");

    assert!(success);
    // Commands should still work despite empty lines
    assert!(stdout.contains("OK"), "Set should succeed: {}", stdout);
    assert!(
        stdout.contains("value1"),
        "Get should return value: {}",
        stdout
    );
}

#[test]
fn test_shell_whitespace_only_lines() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Send whitespace-only lines interspersed with commands
    let (stdout, _, success) = run_shell(
        &db_path,
        "   \n\t\nset key1 value1\n   \n\t\t\nget key1\nexit\n",
    );

    assert!(success);
    // Commands should still work despite whitespace-only lines
    assert!(stdout.contains("OK"), "Set should succeed: {}", stdout);
    assert!(
        stdout.contains("value1"),
        "Get should return value: {}",
        stdout
    );
}

#[test]
fn test_shell_comment_lines() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Send comment lines (shlex treats # as start of comment, resulting in zero tokens)
    let (stdout, _, success) = run_shell(
        &db_path,
        "# This is a comment\nset key1 value1\n# Another comment\nget key1\n#\nexit\n",
    );

    assert!(success);
    // Commands should still work, comments should be ignored
    assert!(stdout.contains("OK"), "Set should succeed: {}", stdout);
    assert!(
        stdout.contains("value1"),
        "Get should return value: {}",
        stdout
    );
}

#[test]
fn test_batch_abort_without_commit() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Begin a batch, set some keys, then abort without committing
    let (stdout, stderr, success) = run_shell(
        &db_path,
        "begin\nset key1 val1\nset key2 val2\nset key3 val3\nabort\n",
    );

    assert!(success);
    // Should see batch started
    assert!(
        stdout.contains("batch started"),
        "Should see batch started: {}",
        stdout
    );
    // Should see batched confirmations
    assert!(
        stdout.contains("batched"),
        "Should see batched confirmations: {}",
        stdout
    );
    // Should warn about uncommitted batch on abort
    assert!(
        stderr.contains("uncommitted") || stdout.contains("uncommitted"),
        "Should warn about uncommitted batch: stdout={}, stderr={}",
        stdout,
        stderr
    );

    // Verify data was NOT persisted - open a new session and check
    let (stdout2, _, success2) = run_shell(&db_path, "get key1\nget key2\nget key3\nexit\n");

    assert!(success2);
    // All keys should not be found
    assert!(
        stdout2.contains("(not found)"),
        "Uncommitted batch data should not persist: {}",
        stdout2
    );
}

#[test]
fn test_blob_tree_with_separation_threshold() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Create a value of 128 bytes (exceeds 64 byte threshold)
    let large_value = "x".repeat(128);

    // Set value with blob tree mode and 64 byte separation threshold
    let (stdout, stderr, success) = run_cli(
        &db_path,
        &["--blob-tree", "--separation-threshold", "64"],
        &["set", "largekey", &large_value],
    );
    assert!(success, "set failed: stderr={}", stderr);
    assert!(stdout.contains("OK"), "Expected OK in output: {}", stdout);

    // Get the value back
    let (stdout, stderr, success) = run_cli(
        &db_path,
        &["--blob-tree", "--separation-threshold", "64"],
        &["get", "largekey"],
    );
    assert!(success, "get failed: stderr={}", stderr);
    assert!(
        stdout.trim() == large_value,
        "Expected value of length {}, got: {}",
        large_value.len(),
        stdout.trim().len()
    );
    assert!(
        stdout.trim() == large_value,
        "Expected '{}', got: '{}'",
        large_value,
        stdout.trim()
    );

    // Scan with -l to see internal details - should show Indirection type after flush
    let (stdout, stderr, success) = run_cli(
        &db_path,
        &["--blob-tree", "--separation-threshold", "64"],
        &["scan", "-l"],
    );
    assert!(success, "scan failed: stderr={}", stderr);
    assert!(
        stdout.contains("type=Indirection"),
        "Expected type=Indirection in output: {}",
        stdout
    );
}

#[test]
fn test_weak_tombstone() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Use shell mode to keep same session across commands
    let (stdout, _, success) = run_shell(
        &db_path,
        "set weakkey weakvalue\ndel --weak weakkey\nscan -l\nflush\nexit\n",
    );
    assert!(success, "shell commands failed: {}", stdout);

    // Verify weak tombstone appears in scan -l output
    // Weak tombstones should show in memtable section or tree statistics
    assert!(
        stdout.contains("weak tombstones") || stdout.contains("type=WeakTombstone"),
        "Expected weak tombstone indication in output: {}",
        stdout
    );

    // After flush, verify weak tombstone count in a new session
    let (stdout, _, success) = run_shell(&db_path, "scan -l\nexit\n");
    assert!(success, "scan failed: {}", stdout);
    assert!(
        stdout.contains("weak tombstones"),
        "Expected weak tombstones count after flush: {}",
        stdout
    );
}

// ============================================================================
// Command line parsting Tests
// ============================================================================

#[test]
fn test_parse_size() {
    let db = temp_db();
    let db_path = db.path().join("test.db");
    let (stdout, stderr, success) = run_cli(&db_path, &["-t", "kaboom"], &[]);
    assert!(!success, "Expected command to fail");
    assert!(
        stderr.contains("invalid digit found in string"),
        "Expected parsing error for non-numeric argument: {}",
        stderr
    );
}

#[test]
fn test_verbose() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    let (stdout, stderr, success) = run_cli(&db_path, &["-q"], &["info"]);
    assert!(success, "Expected command to succeed");

    let (stdout, stderr, success) = run_cli(&db_path, &["-v"], &["info"]);
    assert!(success, "Expected command to succeed");
    assert!(
        stderr.contains("log level: info"),
        "Expected 'log level: info' statement: {}",
        stderr
    );

    let (stdout, stderr, success) = run_cli(&db_path, &["-vv"], &["info"]);
    assert!(success, "Expected command to succeed");
    assert!(
        stderr.contains("log level: debug"),
        "Expected 'log level: debug' statement: {}",
        stderr
    );

    let (stdout, stderr, success) = run_cli(&db_path, &["-vvv"], &["info"]);
    assert!(success, "Expected command to succeed");
    assert!(
        stderr.contains("log level: trace"),
        "Expected 'log level: trace' statement: {}",
        stderr
    );
}

#[test]
fn test_cli_error() {
    let db = temp_db();
    let db_path = db.path().join("test.db");

    // Create DB as Blob Tree
    let (stdout, stderr, success) = run_cli(&db_path, &["-b"], &["info"]);
    assert!(success, "Expected command to succeed");

    // Open DB as regular tree
    let (stdout, stderr, success) = run_cli(&db_path, &[], &["info"]);
    assert!(!success, "Expected command to fail");
    assert!(
        stderr.contains("LsmTreeError: Unrecoverable"),
        "Expected 'LsmTreeError: Unrecoverable': {}",
        stderr
    );
    assert!(
        stderr.contains("Note: Use -v (one or multiple times) for more information"),
        "Expected 'Note: Use -v (one or multiple times) for more information': {}",
        stderr
    );

    // Open DB as regular tree
    let (stdout, stderr, success) = run_cli(&db_path, &["-v"], &["info"]);
    assert!(!success, "Expected command to fail");
    assert!(
        stderr.contains("LsmTreeError: Unrecoverable"),
        "Expected 'LsmTreeError: Unrecoverable': {}",
        stderr
    );
}

// ============================================================================
// Interactive shell tests using rexpect
//
// These tests verify the interactive shell behavior including:
// - Prompt display
// - Command execution
// - Ctrl+C handling
// - Ctrl+D handling
// - History navigation (if supported)
// ============================================================================

#[cfg(unix)]
mod tests_rexpect_unix_only {
    use super::{lsm_binary, temp_db};
    use rexpect::session::PtySession;

    /// Spawn an interactive shell session
    fn spawn_shell(db_path: &std::path::Path) -> Result<PtySession, rexpect::error::Error> {
        let binary = lsm_binary();
        let db_path_str = db_path.to_str().unwrap();
        // Use sh -c to execute the command with arguments
        let command = format!("sh -c '{} {}'", binary.to_str().unwrap(), db_path_str);
        rexpect::spawn(&command, Some(5000))
    }

    #[test]
    fn test_interactive_prompt() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        // Wait for welcome message and prompt
        p.exp_string("Welcome to the LSM-tree shell")
            .expect("Failed to see welcome message");
        p.exp_string("Type 'help' for available commands")
            .expect("Failed to see help message");
        p.exp_regex("lsm> ").expect("Failed to see prompt");

        // Send exit command
        p.send_line("exit")?;
        p.exp_string("OK (flushed)")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_basic_commands() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        // Skip welcome messages
        p.exp_regex("lsm> ")?;

        // Test set command
        p.send_line("set testkey testvalue")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Test get command
        p.send_line("get testkey")?;
        p.exp_string("testvalue")?;
        p.exp_regex("lsm> ")?;

        // Test scan command
        p.send_line("scan")?;
        p.exp_string("testkey = testvalue")?;
        p.exp_string("OK (1 items)")?;
        p.exp_regex("lsm> ")?;

        // Test count command
        p.send_line("count")?;
        p.exp_string("1")?;
        p.exp_regex("lsm> ")?;

        // Exit
        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_delete() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Set a key
        p.send_line("set key1 value1")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Delete it
        p.send_line("del key1")?;
        p.exp_string("OK")?;
        p.exp_regex("lsm> ")?;

        // Verify it's gone
        p.send_line("get key1")?;
        p.exp_string("(not found)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_batch_operations() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Begin batch
        p.send_line("begin")?;
        p.exp_string("OK (batch started)")?;
        p.exp_regex("lsm> ")?;

        // Add operations to batch
        p.send_line("set batch1 val1")?;
        p.exp_string("OK (batched, ready to commit)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("set batch2 val2")?;
        p.exp_string("OK (batched, ready to commit)")?;
        p.exp_regex("lsm> ")?;

        // Get from batch
        p.send_line("get batch1")?;
        p.exp_string("val1")?;
        p.exp_regex("lsm> ")?;

        // Commit batch
        p.send_line("commit")?;
        p.exp_string("OK (batch committed")?;
        p.exp_regex("lsm> ")?;

        // Verify committed
        p.send_line("scan")?;
        p.exp_string("batch1 = val1")?;
        p.exp_string("batch2 = val2")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_batch_rollback() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Set a value outside batch
        p.send_line("set existing value")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Begin batch
        p.send_line("begin")?;
        p.exp_string("OK (batch started)")?;
        p.exp_regex("lsm> ")?;

        // Add to batch
        p.send_line("set batchkey batchvalue")?;
        p.exp_string("OK (batched, ready to commit)")?;
        p.exp_regex("lsm> ")?;

        // Rollback
        p.send_line("rollback")?;
        p.exp_string("OK (batch rolled back)")?;
        p.exp_regex("lsm> ")?;

        // Verify batch key is gone, existing key remains
        p.send_line("get batchkey")?;
        p.exp_string("(not found)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("get existing")?;
        p.exp_string("value")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_info_command() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Add some data
        p.send_line("set key1 value1")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Run info
        p.send_line("info")?;
        p.exp_string("Path:")?;
        p.exp_string("Tables:")?;
        p.exp_string("Approximate items:")?;
        p.exp_string("Disk space:")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_flush() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Set a value
        p.send_line("set flushkey flushvalue")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Flush
        p.send_line("flush")?;
        p.exp_string("OK (flushed)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_quit_alias() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Use quit instead of exit
        p.send_line("quit")?;
        p.exp_string("OK (flushed)")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_abort() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Set a value
        p.send_line("set abortkey abortvalue")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Abort (should not flush)
        p.send_line("abort")?;
        p.exp_eof()?;

        // Verify data was not persisted by opening a new session
        let mut p2 = spawn_shell(&db_path).expect("Failed to spawn shell");
        p2.exp_regex("lsm> ")?;
        p2.send_line("get abortkey")?;
        p2.exp_string("(not found)")?;
        p2.send_line("exit")?;
        p2.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_empty_lines() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Send empty line
        p.send_line("")?;
        p.exp_regex("lsm> ")?;

        // Send command after empty line
        p.send_line("set key value")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_range_command() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Add keys
        p.send_line("set a 1")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("set b 2")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("set c 3")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Range query
        p.send_line("range a c")?;
        p.exp_string("a = 1")?;
        p.exp_string("b = 2")?;
        p.exp_string("(2 items)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_scan_with_prefix() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Add keys with different prefixes
        p.send_line("set user:1 alice")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("set user:2 bob")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("set item:1 widget")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Scan with prefix
        p.send_line("scan user:")?;
        p.exp_string("user:1 = alice")?;
        p.exp_string("user:2 = bob")?;
        p.exp_string("(2 items)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_scan_long() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Set a value
        p.send_line("set testkey testvalue")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Scan with long flag
        p.send_line("scan -l")?;
        p.exp_string("=== Active Memtable ===")?;
        p.exp_string("seqno=")?;
        p.exp_string("type=Value")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_help_command() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Send help command
        p.send_line("help")?;
        // Help output may go to stdout or stderr, but should contain command info
        // We'll just check that we get back to the prompt
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_error_handling() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Try invalid command
        p.send_line("notacommand")?;
        // Should show error but continue
        p.exp_regex("lsm> ")?;

        // Try command with missing args
        p.send_line("set onlykey")?;
        // Should show error but continue
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_batch_info() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Begin batch
        p.send_line("begin")?;
        p.exp_string("OK (batch started)")?;
        p.exp_regex("lsm> ")?;

        // Add operations
        p.send_line("set k1 v1")?;
        p.exp_string("OK (batched, ready to commit)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("del k2")?;
        p.exp_string("OK (batched, ready to commit)")?;
        p.exp_regex("lsm> ")?;

        // Check info shows batch
        p.send_line("info")?;
        p.exp_string("Active batch (2 operations)")?;
        p.exp_string("SET k1 = v1")?;
        p.exp_string("DEL k2")?;
        p.exp_regex("lsm> ")?;

        p.send_line("rollback")?;
        p.exp_string("OK (batch rolled back)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_exit_with_batch_warning() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Begin batch
        p.send_line("begin")?;
        p.exp_string("OK (batch started)")?;
        p.exp_regex("lsm> ")?;

        // Add operation
        p.send_line("set k v")?;
        p.exp_string("OK (batched, ready to commit)")?;
        p.exp_regex("lsm> ")?;

        // Exit with uncommitted batch (should warn)
        p.send_line("exit")?;
        // Warning goes to stderr, but we should still see flush message
        p.exp_string("OK (flushed)")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_weak_delete() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Set a value
        p.send_line("set weakkey weakvalue")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Weak delete
        p.send_line("del --weak weakkey")?;
        p.exp_string("OK")?;
        p.exp_regex("lsm> ")?;

        // Verify with scan -l
        p.send_line("scan -l")?;
        p.exp_string("type=WeakTombstone")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }

    #[test]
    fn test_interactive_compact() -> Result<(), rexpect::error::Error> {
        let db = temp_db();
        let db_path = db.path().join("test.db");

        let mut p = spawn_shell(&db_path).expect("Failed to spawn shell");

        p.exp_regex("lsm> ")?;

        // Add some data
        p.send_line("set k1 v1")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        p.send_line("set k2 v2")?;
        p.exp_string("OK (set)")?;
        p.exp_regex("lsm> ")?;

        // Flush to create tables
        p.send_line("flush")?;
        p.exp_string("OK (flushed)")?;
        p.exp_regex("lsm> ")?;

        // Compact
        p.send_line("compact")?;
        p.exp_string("OK")?;
        p.exp_regex("lsm> ")?;

        p.send_line("exit")?;
        p.exp_eof()?;

        Ok(())
    }
}
