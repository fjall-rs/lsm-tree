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

- **Caller-handled edge cases**: Before flagging a function for not handling an edge case (empty collection, `None` vs `Some(empty)`, missing guard), read ALL call sites. If every caller already handles the case, the function's behavior is part of a deliberate contract — not a bug. Only flag if the edge case is truly unhandled end-to-end.
- **Type-system-prevented issues**: Before flagging a potential collision, overlap, or misuse, check whether distinct enum variants, wrapper types, or visibility modifiers make the issue structurally impossible. A `WeakTombstone` variant that never appears in user-facing merge paths cannot collide with user data regardless of key/seqno overlap.
- **Documented design decisions**: When code has a comment explaining WHY a specific approach was chosen, trust the documented reasoning. Flag only if the comment contradicts the actual code behavior — not if you would have chosen a different approach.

- **Comment wording vs code behavior**: If a comment says "flush when full" but the threshold is checked with `>=` not `>`, the intent is clear — the boundary condition is a design choice. Do not suggest rewording comments to match exact comparison operators.
- **Comment precision**: "returns the block" when it technically returns `Result<Block>` — the comment conveys meaning, not type signature.
- **Magic numbers with context**: `4` in `assert_eq!(header.len(), 4, "expected u32 checksum")` — the assertion message provides the context. Do not suggest a named constant when the value is used once in a test with an explanatory message.
- **Block sizes and compression levels**: Specific numeric values for block sizes (e.g., `4096`), compression levels, or bloom filter parameters are domain constants, not magic numbers, when used in configuration or tests with surrounding context.
- **Segment ID and sequence number formats**: Internal naming conventions for segment files and sequence counters are implementation choices, not review findings.
- **Minor naming preferences**: `lvl` vs `level`, `blk` vs `block`, `seg` vs `segment` — these are team style, not bugs.
- **Import ordering**: Import grouping or ordering style (e.g., std vs crate vs external order). Unused imports are NOT cosmetic — they cause `clippy -D warnings` failures and must be removed.
- **Test code style**: Tests prioritize readability and explicitness over DRY. Repeated setup code in tests is acceptable.
- **`#[allow(clippy::...)]` in untouched legacy code**: Do not flag `#[allow]` on lines outside the PR diff. For new or modified code within the diff, flag `#[allow]` and request migration to `#[expect(..., reason = "...")]`.
- **Temporary directory strategies**: Using `tempfile::tempdir()` vs manual temp paths — both are valid in test code.

## Scope Rules

- **Review ONLY code within the PR's diff.** Do not suggest inline fixes for unchanged lines.
- For issues **outside the diff**, suggest opening a separate issue.
- **Read the PR description.** If it lists known limitations or deferred items, do not re-flag them.
- This fork has **multiple feature branches in parallel**. A hardening that seems missing in one PR may already exist in another. Check the PR description for cross-references.

## Rust-Specific Standards

- `#[expect(lint, reason = "...")]` is the standard for lint suppressions **when the crate’s MSRV is at least Rust 1.79** (the stabilization release for `#[expect]`). `#[expect]` warns when suppression becomes unnecessary, catching stale allowances.
  - For repos with **MSRV ≥ 1.79**: Flag any new `#[allow(lint)]` in the PR diff and request migration to `#[expect(..., reason = "...")]`. `#[allow]` is accepted only for legacy code on lines outside the diff, with a migration note recommended.
  - For repos with **MSRV < 1.79** (where `#[expect]` is unavailable): `#[allow(lint)]` is acceptable, but must be accompanied by a brief `// reason: ...` or `// SAFETY: ...` style comment explaining the justification. Do **not** request migration to `#[expect]` unless/until the repo explicitly raises its MSRV to ≥ 1.79.
- `TryFrom`/`TryInto` for fallible conversions; `as` casts need justification
- No `unwrap()` / `expect()` on I/O paths — use `?` propagation
- `expect()` is acceptable for programmer invariants (e.g., lock poisoning, `const` construction) with reason
- Code must pass `cargo clippy --all-features --all-targets -- -D warnings`

## Testing Standards

- Test naming: `fn <what>_<condition>_<expected>()` (sole exception: `src/compaction/leveled/test.rs` may use `fn test_<scenario>()`)
- Corruption tests: tamper the relevant on-disk field (checksum, block header, segment metadata) and assert the expected error
- Use the same serialization/compression APIs as production; avoid test-only helpers that change framing or length-prefixing.
- Use `tempfile::tempdir()` for test directories — ensures cleanup even on panic
- Integration tests that require specific disk layout or large data use `#[ignore = "reason"]`
- Prefer `assert_eq!` with message over bare `assert!` for better failure output
- Hardcoded values in tests are fine when accompanied by explanatory comments or assertion messages
