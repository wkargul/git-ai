# Hook Catch-All Migration — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand git-ai hook registrations from specific-tool matchers to catch-all matchers, so every tool call (not just file-write tools) triggers pre/post checkpoints. This is a prerequisite for supporting Bash tool checkpointing and any future tool additions without requiring re-installs.

**Constraint (safety-critical):** Users may have their own hooks registered on the same matcher blocks. We must never modify or destroy non-git-ai hooks, and we must never leave doubled git-ai hooks (race conditions). All work must be TDD — tests written first, then implementation.

---

## Affected Agents

Three agents use explicit matcher strings that need migrating:

| Agent | Settings file | Hook type names | Old matcher | New catch-all |
|-------|--------------|-----------------|-------------|---------------|
| **Claude Code** | `~/.claude/settings.json` | PreToolUse / PostToolUse | `"Write\|Edit\|MultiEdit"` | `"*"` |
| **Droid** (Factory) | `~/.factory/settings.json` | PreToolUse / PostToolUse | `"^(Edit\|Write\|Create\|ApplyPatch)$"` | `"*"` |
| **Gemini CLI** | `~/.gemini/settings.json` | BeforeTool / AfterTool | `"write_file\|replace"` | `"*"` |

Agents that require **no changes** (no matcher concept — already catch all tools):

| Agent | Reason no change needed |
|-------|------------------------|
| Cursor | Flat `preToolUse`/`postToolUse` arrays, no matcher field |
| GitHub Copilot | Flat `PreToolUse`/`PostToolUse` arrays, no matcher field |
| Windsurf | Uses event-name hooks (`pre_write_code` etc.), no tool filter |
| Amp | TypeScript plugin `tool.call`/`tool.result` events |
| OpenCode | TypeScript plugin `tool.execute.before`/`tool.execute.after` events |
| Codex | TOML `notify` array, no tool filter |

## Catch-All Matcher Sources

- Claude Code: https://docs.anthropic.com/en/docs/claude-code/hooks — `"*"` matches all tools; `""` and omitting `matcher` also work
- Droid/Factory: https://docs.factory.ai/cli/configuration/hooks-guide — `"*"` matches all tools
- Gemini CLI: https://github.com/google-gemini/gemini-cli/blob/main/docs/hooks/index.md — `"*"` matches all tools

We use `"*"` as the canonical catch-all constant across all three agents.

---

## Migration Algorithm (applies to all three agents)

This is the critical invariant for `install_hooks()`:

### For each hook type (e.g. PreToolUse/PostToolUse):

1. **Strip git-ai from every non-`"*"` matcher block** (migration path; no-op on clean state)
   - Iterate all matcher blocks for this hook type
   - For any block where `matcher != "*"`: remove all entries where `is_git_ai_checkpoint_command(cmd)` is true
   - Do NOT remove the block itself; do NOT touch non-git-ai entries
2. **Find or create the `"*"` matcher block**
3. **Ensure exactly one git-ai command** in the `"*"` block:
   - If none found → append new hook entry
   - If one found and command string matches desired → leave as-is
   - If one found and command string is stale → update it in-place
   - If multiple found → keep first, drop rest (deduplication)
4. **No empty-block cleanup** — leave empty matcher blocks as-is; don't second-guess user intent

### `uninstall_hooks()` — no algorithm change needed

Already iterates **all** matcher blocks and removes git-ai commands. Works correctly for both old and new matchers.

### `check_hooks()` — update `hooks_up_to_date` for each agent

- `hooks_installed` = git-ai command exists in **any** matcher block (allows detection of pre-migration state)
- `hooks_up_to_date` = git-ai command exists specifically in the `"*"` matcher block

---

## New Constants (per agent file)

```rust
// Replace old specific-tool constants
const CLAUDE_CATCH_ALL_MATCHER: &str = "*";
const DROID_CATCH_ALL_MATCHER: &str = "*";
const GEMINI_CATCH_ALL_MATCHER: &str = "*";
```

---

## Test Scenarios

All tests invoke the real `install_hooks()` / `uninstall_hooks()` / `check_hooks()` methods (via a temp-dir home override, not hand-rolled JSON). Each agent gets its own test suite covering the same scenario matrix.

### Helper pattern (same for each agent)

```rust
fn run_install(settings_path: &Path, binary_path: &Path) -> Option<String> { ... }
fn run_uninstall(settings_path: &Path) -> Option<String> { ... }
fn assert_git_ai_only_in_catch_all(settings: &Value) { ... }
```

### Install scenarios (write as failing tests first)

- [ ] **S1: Fresh install** — no settings.json → creates file with `"*"` matcher block containing git-ai hook for all hook types
- [ ] **S2: Idempotent** — git-ai already in `"*"` block → `install_hooks` returns `Ok(None)` (no diff)
- [ ] **S3: Migration, no user hooks** — git-ai in old matcher block only, block has no other hooks → git-ai moved to `"*"` block, old block still present but empty
- [ ] **S4: Migration, user hook in same old block** — old block has `["user-cmd", git-ai-cmd]` → git-ai moved to `"*"` block; old block retains `["user-cmd"]` untouched
- [ ] **S5: Fresh install, user has hook on old matcher** — old block has `["user-cmd"]`, no git-ai → `"*"` block created with git-ai; old block unchanged
- [ ] **S6: Fresh install, user has their own `"*"` catch-all hook** — `"*"` block has `["user-cmd"]` → git-ai appended: `["user-cmd", git-ai-cmd]`
- [ ] **S7: Idempotent with user catch-all hook** — `"*"` block has `["user-cmd", git-ai-cmd]` → returns `Ok(None)`
- [ ] **S8: Deduplication across blocks** — git-ai in both `"*"` and old matcher → git-ai only in `"*"`, removed from old; user hooks in old block preserved
- [ ] **S9: Deduplication within `"*"` block** — `"*"` block has two git-ai entries → reduced to exactly one
- [ ] **S10: Stale command upgrade** — git-ai in `"*"` block with wrong path → updated to current binary path
- [ ] **S11: Git-ai in arbitrary old matcher** — git-ai in some other old matcher (not the specific old one) → git-ai moved to `"*"`, arbitrary old matcher's user hooks preserved
- [ ] **S12: Git-ai spread across multiple old blocks** — git-ai in two non-`"*"` blocks → both cleaned, single entry in `"*"`
- [ ] **S13: Hook types handled independently** — PreToolUse migrated, PostToolUse already on `"*"` → only PreToolUse changes; PostToolUse untouched

### Uninstall scenarios

- [ ] **U1: Uninstall when on catch-all** — `"*"` block has git-ai → removed; user hooks in block preserved
- [ ] **U2: Uninstall when on old matcher (pre-migration)** — old matcher has `["user-cmd", git-ai-cmd]` → user-cmd preserved, git-ai removed
- [ ] **U3: Uninstall from multiple blocks** — git-ai in both `"*"` and old matcher → removed from both; user hooks preserved everywhere
- [ ] **U4: No-op uninstall** — no git-ai commands anywhere → returns `Ok(None)`

### check_hooks scenarios

- [ ] **C1: No hooks** → `hooks_installed: false`, `hooks_up_to_date: false`
- [ ] **C2: Git-ai in `"*"` block** → `hooks_installed: true`, `hooks_up_to_date: true`
- [ ] **C3: Git-ai in old matcher only** → `hooks_installed: true`, `hooks_up_to_date: false` (triggers migration on next `mdm install`)

---

## Critical Implementation Snippet (same logic for all three agents)

```rust
// Step 1: Strip git-ai from all non-catch-all blocks
for block in hook_type_array.iter_mut() {
    let is_catch_all = block.get("matcher")
        .and_then(|m| m.as_str())
        .map(|m| m == CATCH_ALL_MATCHER)
        .unwrap_or(false);
    if !is_catch_all {
        if let Some(hooks) = block.get_mut("hooks").and_then(|h| h.as_array_mut()) {
            hooks.retain(|hook| {
                hook.get("command").and_then(|c| c.as_str())
                    .map(|cmd| !is_git_ai_checkpoint_command(cmd))
                    .unwrap_or(true)
            });
        }
    }
}

// Steps 2-3: Find or create "*" block, then ensure exactly one git-ai entry
let catch_all_idx = hook_type_array.iter()
    .position(|b| b.get("matcher").and_then(|m| m.as_str()) == Some(CATCH_ALL_MATCHER))
    .unwrap_or_else(|| {
        hook_type_array.push(json!({"matcher": CATCH_ALL_MATCHER, "hooks": []}));
        hook_type_array.len() - 1
    });
// ... dedup + add/update logic (same pattern as current code, applied to catch_all_idx block)
```

---

## Implementation Steps

- [x] **1. Claude Code tests** — write all S1–S13, U1–U4, C1–C3 as failing tests in `claude_code.rs`
- [x] **2. Claude Code implementation** — add `CLAUDE_CATCH_ALL_MATCHER`, rewrite `install_hooks` inner loop, update `check_hooks`; run tests until green
- [x] **3. Update existing Claude Code tests** — replaced old hand-rolled tests with real install_hooks_at calls using `matcher: "*"`
- [x] **4. Droid tests** — write same S1–S13, U1–U4, C1–C3 matrix as failing tests in `droid.rs`
- [x] **5. Droid implementation** — same algorithm, `DROID_CATCH_ALL_MATCHER = "*"`, old matcher was `"^(Edit|Write|Create|ApplyPatch)$"`
- [x] **6. Update existing Droid tests** — replaced with real install_hooks_at calls using `matcher: "*"`
- [x] **7. Gemini tests** — write same matrix as failing tests in `gemini.rs` (note: hook type names are `BeforeTool`/`AfterTool`)
- [x] **8. Gemini implementation** — same algorithm, `GEMINI_CATCH_ALL_MATCHER = "*"`, old matcher was `"write_file|replace"`; `tools.enableHooks: true` still set
- [x] **9. Update existing Gemini tests** — replaced with real install_hooks_at calls using `matcher: "*"`
- [x] **10. Full test run** — `cargo test -p git-ai` — 135 agent tests pass, full suite clean
- [x] **11. OpenCode plugin** — removed `FILE_EDIT_TOOLS` filter and `isEditTool` guard; plugin now fires `tool.execute.before`/`tool.execute.after` for ALL tool calls (not just file-edit tools); updated test assertion
- [x] **12. Amp plugin** — already catches ALL tool calls via `tool.call`/`tool.result` events with no tool-name filter; no changes needed
- [x] **13. Other agents confirmed** — Cursor, GitHub Copilot: flat arrays, no matcher; Windsurf: event-type hooks; Codex: TOML notify array; all catch every tool call already

---

## Verification

1. `cargo test -p git-ai -- claude_code` — all new + existing tests pass
2. `cargo test -p git-ai -- droid` — all new + existing tests pass
3. `cargo test -p git-ai -- gemini` — all new + existing tests pass
4. Manual — create settings.json with old matcher + user hook + git-ai, run `git-ai mdm install`, confirm user hook preserved and git-ai is on `"*"` matcher
5. Manual — run install twice → no diff on second run (idempotent)
6. Manual — run uninstall → git-ai gone, user hooks intact
7. `cargo test -p git-ai` — full suite passes
