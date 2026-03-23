# GitHub Copilot Instructions for coordinode-lsm-tree (structured-world fork)

## Project Overview

This is a **maintained fork** of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree) — a K.I.S.S. LSM-tree implementation in Rust. We maintain additional features and hardening for the [CoordiNode](https://github.com/structured-world/coordinode) database engine while contributing patches upstream.

## Review Scope Rules (CRITICAL)

**Review ONLY code within the PR's diff.** For issues found in code outside the diff:
- Do NOT suggest inline code fixes for unchanged lines
- Instead, suggest creating a **separate issue** with the finding (e.g., "Consider opening an issue to add size validation in `from_reader` — this is outside the scope of this clippy-fix PR")

**Each PR has a defined scope in its description.** Read the "out of scope" section before reviewing. If something is listed as out of scope, do not flag it — it is tracked in another PR.

**Cross-PR awareness:** This fork has multiple feature branches in parallel. If a hardening or feature seems missing, check whether it exists in another open PR before suggesting it. Reference the other PR number if known.

**Prefer issue suggestions over code suggestions for out-of-scope findings.** This keeps PRs focused and reviewable.

## Rust Code Standards

- **Unsafe code:** Prefer safe alternatives. If `unsafe` is required, it must have a `// SAFETY:` comment explaining the invariant.
- **Error handling:** No `unwrap()` or `expect()` on I/O paths. Use `Result<T, E>` propagation. `expect()` is acceptable for programmer invariants (e.g., lock poisoning) with `#[expect(clippy::expect_used, reason = "...")]`.
- **Clippy:** Code must pass `cargo clippy --all-features -- -D warnings`. Use `#[expect(...)]` (not `#[allow(...)]`) for justified suppressions — `#[expect]` warns if the suppression becomes unnecessary.
- **Casts:** Prefer `TryFrom`/`TryInto` for fallible conversions. `as` casts are acceptable for infallible cases (e.g., `u32` to `u64`) with `#[expect(clippy::cast_possible_truncation)]` and a reason.
- **Feature gates:** Code behind `#[cfg(feature = "...")]` must compile with any combination of features. Variables used only in feature-gated branches must also be feature-gated.
- **Definite initialization:** `let x: T;` without a default value is valid Rust — the compiler tracks initialization per control-flow path and does not drop uninitialized bindings; do not flag this as a bug when the variable is only used in paths that initialize it.

## Testing Standards

- **Corruption tests:** When adding validation for on-disk data, add a test that tampers the relevant field and asserts the error. Use the same serialization path as production (e.g., `lz4_flex::compress` not `compress_prepend_size`).
- **No mocks for storage:** Tests use real on-disk files via `tempfile::tempdir()`.
- **Test naming:** `fn <what>_<condition>_<expected>()` — e.g., `fn lz4_corrupted_header_triggers_decompress_error()`.

## Commit Message Format

```
<type>(scope): <description>

- Detail 1
- Detail 2
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `style`, `chore`, `perf`, `ci`, `build`, `revert`

**Forbidden patterns:** "address review", "fix PR comments", "WIP", "temporary"

## Build and Test

```bash
cargo clippy --all-features -- -D warnings  # Lint (strict)
cargo test --features lz4                    # Tests with lz4
cargo test --all-features                    # Tests with all features
cargo fmt --all -- --check                   # Format check
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `lz4` | LZ4 compression (enabled by default in fjall) |
| `zstd` | Zstd compression (PR #1) |
| `bytes_1` | Use `bytes` crate for Slice type |
| `metrics` | Expose prometheus metrics |

## Design Decision Analysis (CRITICAL)

**Before flagging any potential issue, trace the call chain visible in the PR diff:**

1. **Read the caller** — does the caller already handle the case you're about to flag? If a function returns a value that looks problematic in isolation, check call sites visible in the diff. The caller may handle the edge case explicitly (e.g., empty collections, `None` vs `Some(empty)`).
2. **Check type-level guarantees** — does the type system prevent the issue? Different enum variants, wrapper types, or visibility modifiers may make a "collision" or "misuse" structurally impossible.
3. **Read adjacent comments** — comments starting with "NOTE:", "Use X instead of Y because...", or explaining WHY a specific approach was chosen document deliberate design decisions. If the comment accurately describes the code behavior, the design is intentional.

**Only suppress Tier 3/Tier 4 findings if they fail these checks.** If the pattern still indicates a Tier 1 (logic/correctness) or Tier 2 (safety/crash) issue, flag it regardless of documented rationale or caller handling.

## Architecture Notes

- `src/table/block/` — On-disk block format (header + compressed payload)
- `src/vlog/blob_file/` — Value log for large values (separate from LSM blocks)
- `src/compaction/` — Compaction strategies (leveled, FIFO, tiered)
- `src/seqno.rs` — Sequence number generator (MVCC versioning)
- `src/range_tombstone.rs` — Range tombstone data model and serialization
- `src/range_tombstone_filter.rs` — MVCC-aware range tombstone filtering for iterators
- `src/active_tombstone_set.rs` — Tracks active range tombstones during compaction
- `src/memtable/interval_tree.rs` — Interval tree for memtable range tombstone queries
- Compression is pluggable via `CompressionType` enum with `#[cfg(feature)]` variants
