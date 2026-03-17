---
applyTo: "**/*.rs"
---

# Rust Code Review Instructions

## Review Priority (HIGH → LOW)

Focus review effort on real bugs, not cosmetics. Stop after finding issues in higher tiers — do not pad reviews with low-priority nitpicks.

### Tier 1 — Logic Bugs and Correctness (MUST flag)
- Data corruption: wrong compaction merge logic, incorrect key ordering, dropped or duplicated entries during merge
- Off-by-one in block/segment boundaries, fence pointer lookups, or range scans
- CRC/checksum mismatches: computing checksum over wrong byte range, verifying against stale value
- TOCTOU on file operations: checking file existence then opening, or reading metadata then acting on it without holding a lock
- Incorrect merge semantics: tombstones not propagated to lower levels, point deletes applied out of order
- Missing validation: unchecked block offset, unvalidated segment metadata from disk
- Resource leaks: unclosed file handles, temporary files not cleaned up on error paths
- Concurrency: data races on shared segment lists, lock ordering violations, missing synchronization on manifest updates
- Error swallowing: `let _ = fallible_call()` silently dropping I/O errors that affect data integrity
- Integer overflow/truncation on sizes, offsets, or block counts

### Tier 2 — Safety and Crash Recovery (MUST flag)
- `unsafe` without `// SAFETY:` invariant explanation
- `unwrap()`/`expect()` on disk I/O or deserialization (must use `Result` propagation)
- Crash safety: write ordering that leaves data unrecoverable after power loss (e.g., updating index before data is fsynced, deleting old segments before new manifest is durable)
- Partial write exposure: readers seeing a segment file that is still being written
- fsync ordering: metadata (manifest, WAL) must be durable before the operation it describes is considered committed
- Hardcoded secrets, credentials, or private URLs

### Tier 3 — API Design and Robustness (flag if clear improvement)
- Public API missing `#[must_use]` on builder-style methods returning `Self` or other non-`Result` types that callers might accidentally discard
- `pub` visibility where `pub(crate)` suffices
- Missing `Send + Sync` bounds on types used across threads
- `Clone` on large types (segment readers, block caches) where a reference would work
- Fallible operations returning `()` instead of `Result`

### Tier 4 — Style (ONLY flag if misleading or confusing)
- Variable/function names that actively mislead about behavior
- Dead code (unused functions, unreachable branches)

## DO NOT Flag (Explicit Exclusions)

These are not actionable review findings. Do not raise them:

- **Comment wording vs code behavior**: If a comment says "flush when full" but the threshold is checked with `>=` not `>`, the intent is clear — the boundary condition is a design choice. Do not suggest rewording comments to match exact comparison operators.
- **Comment precision**: "returns the block" when it technically returns `Result<Block>` — the comment conveys meaning, not type signature.
- **Magic numbers with context**: `4` in `assert_eq!(header.len(), 4, "expected u32 checksum")` — the assertion message provides the context. Do not suggest a named constant when the value is used once in a test with an explanatory message.
- **Block sizes and compression levels**: Specific numeric values for block sizes (e.g., `4096`), compression levels, or bloom filter parameters are domain constants, not magic numbers, when used in configuration or tests with surrounding context.
- **Segment ID and sequence number formats**: Internal naming conventions for segment files and sequence counters are implementation choices, not review findings.
- **Minor naming preferences**: `lvl` vs `level`, `blk` vs `block`, `seg` vs `segment` — these are team style, not bugs.
- **Import ordering**: Import grouping or ordering style (e.g., std vs crate vs external order). Unused imports are NOT cosmetic — they cause `clippy -D warnings` failures and must be removed.
- **Test code style**: Tests prioritize readability and explicitness over DRY. Repeated setup code in tests is acceptable.
- **`#[allow(clippy::...)]` in existing upstream code**: Existing `#[allow]` suppressions with justification comments are legacy — do not flag in unchanged code. New code in PRs should use `#[expect(clippy::...)]` per Rust Standards below.
- **Temporary directory strategies in existing code**: Existing tests using manual temp paths instead of `tempfile::tempdir()` are not a finding. New tests should prefer `tempfile::tempdir()` per Testing Standards below.

## Scope Rules

- **Review ONLY code within the PR's diff.** Do not suggest inline fixes for unchanged lines.
- For issues **outside the diff**, suggest opening a separate issue.
- **Read the PR description.** If it lists known limitations or deferred items, do not re-flag them.
- This fork has **multiple feature branches in parallel**. A hardening that seems missing in one PR may already exist in another. Check the PR description for cross-references.

## Rust-Specific Standards

- Prefer `#[expect(lint)]` over `#[allow(lint)]` — `#[expect]` warns when suppression becomes unnecessary
- `TryFrom`/`TryInto` for fallible conversions; `as` casts need justification
- No `unwrap()` / `expect()` on I/O paths — use `?` propagation
- `expect()` is acceptable for programmer invariants (e.g., lock poisoning, `const` construction) with reason
- Code must pass `cargo clippy --all-features -- -D warnings`

## Testing Standards

- Test naming: `fn <what>_<condition>_<expected>()` or `fn test_<scenario>()`
- Corruption tests: tamper the relevant on-disk field (checksum, block header, segment metadata) and assert the expected error
- Use same serialization as production (e.g., `lz4_flex::compress` not `compress_prepend_size`)
- Use `tempfile::tempdir()` for test directories — ensures cleanup even on panic
- Integration tests that require specific disk layout or large data use `#[ignore = "reason"]`
- Prefer `assert_eq!` with message over bare `assert!` for better failure output
- Hardcoded values in tests are fine when accompanied by explanatory comments or assertion messages
