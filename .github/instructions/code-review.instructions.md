---
applyTo: "**/*.rs"
---

# Code Review Instructions for lsm-tree (structured-world fork)

## Scope Rules (CRITICAL)

- **Review ONLY code within the PR's diff.** Do not suggest inline fixes for unchanged lines.
- For issues in code **outside the diff**, suggest creating a **separate issue** instead of proposing code changes. Example: "Consider opening an issue to add size validation here — this is outside this PR's scope."
- **Read the PR description carefully.** If the PR body has an "out of scope" section listing items handled by other PRs, do NOT flag those items.
- This fork has **multiple feature branches in parallel**. A hardening that seems missing in one PR may already exist in another open PR. Check the "out of scope" section for cross-references.

## Rust Standards

- `unsafe` blocks require `// SAFETY:` comments explaining the invariant
- Prefer `#[expect(lint)]` over `#[allow(lint)]` — `#[expect]` warns when suppression becomes unnecessary
- Use `TryFrom`/`TryInto` for fallible conversions; `as` casts need `#[expect(clippy::cast_possible_truncation)]` with reason
- No `unwrap()` / `expect()` on I/O paths — use `Result` propagation
- `expect()` is acceptable for programmer invariants (lock poisoning) with `#[expect(clippy::expect_used, reason = "...")]`
- Code must pass `cargo clippy --all-features -- -D warnings`

## Testing

- Corruption/validation tests: tamper the relevant on-disk field and assert the error
- Use same serialization as production (e.g., `lz4_flex::compress` not `compress_prepend_size`)
- Test naming: `fn <what>_<condition>_<expected>()`
