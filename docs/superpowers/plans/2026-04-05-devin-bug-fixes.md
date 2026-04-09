# Devin Review Bug Fixes — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address all open bugs and warnings flagged by Devin Review on PR #798 that were not already fixed by previous commits (c3369d4d resolved `repo_working_dir: None` regressions and ContinueCli `tool_use_id` hardcoding; those are done).

**Note on earlier findings:** Several 2026-03-25 Devin findings have already been fixed in the current codebase and do not require work here:
- `git_status_fallback` path-with-spaces: fixed (`splitn(n, ' ')` with correct field count)
- Snapshot walker disabling gitignore: fixed (`git_ignore(true)`, `git_global(true)`, `git_exclude(true)`)
- `build_gitignore` depth-1 limit: fixed (recursive with `MAX_GITIGNORE_DEPTH=10` and 2s deadline)
- `filter_entry` absolute-path `.git` check: fixed (`entry.file_name() != ".git"`)
- Symlinks skipped via `is_dir()`: fixed (`entry.file_type()` lstat semantics)
- Redundant `git_status_fallback` on `Fallback` arm: already returns `None`, no second call

---

## Issue Inventory

| # | Severity | File | Description |
|---|----------|------|-------------|
| B1 | 🔴 | `agent_presets.rs:874` | GeminiPreset (and others) hardcode `"bash"` as `tool_use_id` fallback → snapshot key collisions for sequential bash calls |
| B2 | 🔴 | `bash_tool.rs:1107` | Post-hook passes deleted files in `edited_filepaths` to `prepare_captured_checkpoint` |
| W1 | 🟡 | `daemon.rs:1302` | `compute_watermarks_from_stat` uses `metadata()` (follows symlinks); snapshot uses `symlink_metadata()` |
| W2 | 🟡 | `bash_tool.rs:942` | Cold-start stale-file dedup uses un-normalized paths → misses duplicates on case-insensitive FS |
| W3 | 🟡 | `daemon.rs:5307` | Worktree watermark never set for bare-CLI `git-ai checkpoint` calls (always has non-empty file list) |
| W4 | 🟡 | `flake.nix:39` | Version `"1.2.4"` does not match `Cargo.toml` `"1.2.6"` |

---

## Fix B1 — GeminiPreset `tool_use_id` collision

### Problem

`agent_presets.rs:870–874` (Gemini) and `:154–158` (Claude), `:1043–1046` (ContinueCli), `:3077–3080` (fourth preset) all fall back to the literal string `"bash"` when no `tool_use_id` field is present in the hook payload:

```rust
let tool_use_id = hook_data
    .get("tool_use_id")
    .or_else(|| hook_data.get("toolUseId"))
    .and_then(|v| v.as_str())
    .unwrap_or("bash");   // ← all sequential bash calls share the same snapshot key
```

The snapshot cache key is `{session_id}:{tool_use_id}`. When two bash calls share the same session and both fall through to `"bash"`, the second call's pre-snapshot overwrites the first, so the first post-hook diffs against the wrong baseline.

**Claude Code always provides `tool_use_id`** in its hook payload, so the fallback never fires in practice. **Gemini CLI's** `BeforeTool`/`AfterTool` hook payload needs to be verified — if Gemini does not include a per-call unique identifier, the hardcoded fallback is dangerous.

### Investigation step

Check whether Gemini CLI's hook payload includes a per-call unique identifier. Look at:
1. The `agent-support/gemini/git-ai.sh` or similar hook script (if present) for what fields are forwarded
2. The `GeminiPreset` test fixtures in `agent_presets.rs` for what hook data is simulated
3. Gemini CLI docs / source — the hook payload schema

### Fix (conditional on investigation result)

**Case A — Gemini provides a unique ID under a different key** (e.g., `callId`, `invocationId`): add that key to the extraction chain before `.unwrap_or("bash")`.

**Case B — Gemini provides no per-call ID**: generate a UUID at pre-hook time and store it in a per-session "last-used ID" sidecar file alongside the snapshot. At post-hook time, read the most-recently-written key for that session. This is safe because Gemini serializes tool calls (no concurrent bash calls within one session).

```rust
// If no ID in hook data, generate a new one for pre-hook and persist it;
// for post-hook, read back the persisted key.
fn resolve_tool_use_id(hook_data: &Value, repo_root: &Path, is_pre: bool) -> String {
    if let Some(id) = hook_data.get("tool_use_id").or_else(|| hook_data.get("toolUseId"))
        .and_then(|v| v.as_str())
    {
        return id.to_string();
    }
    // Fallback: use a sidecar file to correlate pre/post hooks
    let sidecar = snapshot_dir(repo_root).join("last_bash_tool_use_id");
    if is_pre {
        let id = uuid::Uuid::new_v4().to_string();
        let _ = fs::write(&sidecar, &id);
        id
    } else {
        fs::read_to_string(&sidecar).unwrap_or_else(|_| "bash".to_string())
    }
}
```

Apply the same fix consistently to all four presets that currently use `.unwrap_or("bash")`.

### Tests
- Test that two sequential Gemini bash calls in the same session each get a distinct snapshot key (pre-snapshot for call 1 is not overwritten by pre-snapshot for call 2)
- Test that the sidecar file correlates pre and post hooks correctly when no `tool_use_id` is in the payload

---

## Fix B2 — Deleted files in post-hook `edited_filepaths`

### Problem

`attempt_post_hook_capture` at `bash_tool.rs:1107` passes the raw `changed_paths` slice (from `diff_result.all_changed_paths()`) directly as `edited_filepaths`:

```rust
let agent_run_result = AgentRunResult {
    ...
    edited_filepaths: Some(changed_paths.to_vec()),  // ← includes deleted files
    dirty_files: Some(contents),                      // ← deleted files absent (ENOENT)
    ...
};
```

`diff_result.all_changed_paths()` includes deleted files. `capture_file_contents` already skips them (returns empty `dirty_files` entry for ENOENT), so `dirty_files` does not contain them. When `prepare_captured_checkpoint` processes `edited_filepaths`, it tries to read files that no longer exist, likely causing silent failures or incorrect checkpoint content.

### Fix

Filter `changed_paths` into two sets before constructing the `AgentRunResult`:

```rust
fn attempt_post_hook_capture(
    repo_root: &Path,
    changed_paths: &[String],
) -> Option<CapturedCheckpointInfo> {
    ...
    // Separate existing files (need content capture) from deleted files
    // (should still be recorded as changed but have no capturable content).
    let (existing_paths, deleted_paths): (Vec<_>, Vec<_>) = changed_paths
        .iter()
        .partition(|p| repo_root.join(p).exists());

    let path_bufs: Vec<PathBuf> = existing_paths.iter().map(PathBuf::from).collect();
    let contents = capture_file_contents(repo_root, &path_bufs);

    let agent_run_result = AgentRunResult {
        ...
        edited_filepaths: Some(existing_paths.iter().chain(&deleted_paths).cloned().collect()),
        dirty_files: Some(contents),
        ...
    };
    ...
}
```

Alternatively, if `prepare_captured_checkpoint` handles missing files gracefully (ENOENT is not fatal), the simpler fix is just to pass all paths as `edited_filepaths` but ensure `prepare_captured_checkpoint` tolerates absent files. Confirm which invariant is expected by the checkpoint processor before choosing which approach to take.

### Tests
- Test that `attempt_post_hook_capture` succeeds when `changed_paths` includes a deleted file
- Verify that the resulting captured checkpoint correctly records both modified and deleted paths

---

## Fix W1 — `compute_watermarks_from_stat` symlink inconsistency

### Problem

`daemon.rs:1302` uses `std::fs::metadata` (follows symlinks — reads target mtime) to compute watermarks, while the snapshot at `bash_tool.rs:494` uses `fs::symlink_metadata` (does not follow symlinks — reads the symlink's own mtime). For symlinked files, these return different timestamps, causing the watermark comparison in `find_stale_files` to be incorrect.

### Fix

Change line 1302 from:
```rust
if let Ok(metadata) = std::fs::metadata(&full_path)
```
to:
```rust
if let Ok(metadata) = std::fs::symlink_metadata(&full_path)
```

### Tests
- Add a test with a symlinked file in the repo; verify that the watermark matches the snapshot mtime and the file is not incorrectly marked stale after a checkpoint

---

## Fix W2 — Cold-start dedup on case-insensitive filesystems

### Problem

`bash_tool.rs:938–944`: in the cold-start branch of `attempt_pre_hook_capture`, git status paths are added to `stale_files`. The dedup check uses:

```rust
let p = PathBuf::from(&path_str);   // original case from git status
if !stale_files.contains(&p) {      // stale_files has normalize_path() entries (lowercased on macOS)
    stale_files.push(p);
}
```

`stale_files` is populated by `find_stale_files` which uses `normalize_path()`-keyed entries (lowercased on macOS/Windows). `PathBuf::from("File.txt") != PathBuf::from("file.txt")`, so duplicates are missed and the same file ends up in `stale_files` twice, captured twice.

### Fix

Normalize `path_str` before constructing `p` and before the duplicate check:

```rust
let normalized_str = crate::utils::normalize_to_posix(&path_str);
let p = normalize_path(Path::new(&normalized_str));
if wm.per_file.contains_key(&normalized_str) {
    continue;
}
if !stale_files.contains(&p) {
    stale_files.push(p);
}
```

This ensures the case-folded paths from `find_stale_files` and the original-case paths from `git_status_fallback` compare correctly.

### Tests
- Unit test: simulate a snapshot with lowercased key `"src/foo.rs"`, a git status output returning `"src/Foo.rs"`, and a per-file watermark miss → verify `stale_files` contains exactly one entry, not two

---

## Fix W3 — Worktree watermark never set for bare-CLI checkpoints

### Problem

`daemon.rs:5307–5308`:

```rust
let is_full_human_checkpoint =
    is_live_human_checkpoint && checkpoint_file_paths.is_empty();
```

For bare `git-ai checkpoint` CLI invocations, `git_ai_handlers.rs:872` calls `get_all_files_for_mock_ai`, which always returns a non-empty file list. This populates `will_edit_filepaths`, which is then used as `checkpoint_file_paths`. Since `checkpoint_file_paths` is never empty for this path, `is_full_human_checkpoint` is never true, and the worktree watermark is never set.

As a result, `find_stale_files` tier 2 (worktree watermark fallback) never fires from this path. Cold-start in `attempt_pre_hook_capture` always falls back to `git status` even when a recent bare-CLI checkpoint exists.

### Proposed fix

Separate the concept of "full sweep" from "no files in payload." Set the worktree watermark for every live Human checkpoint (not just scoped-empty ones), so that the bash tool always has a baseline:

```rust
// Set per-worktree watermark for every live human checkpoint regardless of scope.
// This gives the bash pre-hook a baseline to detect files modified since the last
// human checkpoint, including when the checkpoint was a full sweep via CLI.
let per_worktree = if is_live_human_checkpoint {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::collections::HashMap::from([(repo_wd.clone(), now_ns)])
} else {
    std::collections::HashMap::new()
};
```

**Caution:** verify this does not cause false-positive "stale" detection. The per-file watermarks from the same checkpoint will still take precedence (Tier 1), so only files with no per-file watermark (i.e., not explicitly listed in that checkpoint) will be evaluated against the worktree watermark. For a full-sweep CLI checkpoint, those are files that weren't in the repo when the checkpoint ran — newly created files — which should indeed be checked.

### Tests
- Test that after a bare `git-ai checkpoint` CLI call, the daemon sets a worktree watermark
- Test that a subsequent bash pre-hook sees the worktree watermark and skips `git status` (Tier 2 covers newly modified files)

---

## Fix W4 — flake.nix version mismatch

### Problem

`flake.nix:39` says `version = "1.2.4"` but `Cargo.toml:3` says `version = "1.2.6"`. The Nix package reports the wrong version at runtime.

### Fix

```nix
# flake.nix:39
version = "1.2.6";
```

No tests needed; this is a string update.

---

## Implementation Order

- [ ] **1. W4 (flake.nix)** — trivial one-liner, do first
- [ ] **2. W1 (symlink_metadata)** — one-liner in daemon.rs, add test
- [ ] **3. W2 (case-insensitive dedup)** — small fix in bash_tool.rs, add test
- [ ] **4. B2 (deleted files in post-hook)** — read `prepare_captured_checkpoint` to confirm its file-missing behavior, then apply appropriate filter; add test
- [ ] **5. B1 (tool_use_id collision)** — investigate Gemini hook payload, implement fix (sidecar or key lookup), add tests
- [ ] **6. W3 (worktree watermark)** — investigate `git_ai_handlers.rs` surrounding context, apply and test
- [ ] **7. Full test run** — `cargo test -p git-ai`
