use crate::git::refs::{
    AI_AUTHORSHIP_PUSH_REFSPEC, AI_SHARDED_NOTES_PREFIX, copy_ref, fallback_merge_notes_ours,
    merge_notes_from_ref, ref_exists, tracking_ref_for_remote,
};
use crate::{
    error::GitAiError,
    git::{cli_parser::ParsedGitInvocation, repository::exec_git},
    utils::debug_log,
};

use super::repository::Repository;

#[cfg(windows)]
fn disabled_hooks_config() -> &'static str {
    "core.hooksPath=NUL"
}

#[cfg(not(windows))]
fn disabled_hooks_config() -> &'static str {
    "core.hooksPath=/dev/null"
}

fn sharded_notes_enabled() -> bool {
    crate::config::Config::get()
        .get_feature_flags()
        .sharded_notes
}

/// Tracking ref prefix for sharded notes from a specific remote.
/// e.g. "refs/notes/ai-s-remote/origin/ab"
fn shard_tracking_ref_prefix(remote_name: &str) -> String {
    format!(
        "refs/notes/ai-s-remote/{}/",
        sanitize_remote_name_for_ref(remote_name)
    )
}

/// Sanitize a remote name for use in a ref path (same logic as refs.rs).
fn sanitize_remote_name_for_ref(remote: &str) -> String {
    remote
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// List all shard tracking refs for a remote (refs/notes/ai-s-remote/<remote>/*).
fn list_shard_tracking_refs(
    repository: &Repository,
    remote_name: &str,
) -> Result<Vec<(String, String)>, GitAiError> {
    let prefix = shard_tracking_ref_prefix(remote_name);
    let mut args = repository.global_args_for_exec();
    args.push("for-each-ref".to_string());
    args.push("--format=%(refname)".to_string());
    args.push(prefix.clone());

    match exec_git(&args) {
        Ok(output) => {
            let stdout = String::from_utf8(output.stdout)
                .map_err(|_| GitAiError::Generic("bad utf8".to_string()))?;
            Ok(stdout
                .lines()
                .filter(|l| !l.is_empty())
                .filter_map(|tracking_ref| {
                    // Extract shard suffix (e.g. "ab") from tracking ref
                    let shard = tracking_ref.strip_prefix(&prefix)?;
                    let local_shard_ref = format!("{}{}", AI_SHARDED_NOTES_PREFIX, shard);
                    Some((tracking_ref.to_string(), local_shard_ref))
                })
                .collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

/// Merge all shard tracking refs into their corresponding local shard refs.
fn merge_shard_tracking_refs(repository: &Repository, remote_name: &str) {
    let shard_refs = match list_shard_tracking_refs(repository, remote_name) {
        Ok(refs) => refs,
        Err(e) => {
            debug_log(&format!("failed to list shard tracking refs: {}", e));
            return;
        }
    };

    for (tracking_ref, local_shard_ref) in &shard_refs {
        if !ref_exists(repository, tracking_ref) {
            continue;
        }

        if ref_exists(repository, local_shard_ref) {
            // Both exist — merge using notes merge -s ours on the shard ref
            let shard_name = local_shard_ref
                .strip_prefix("refs/notes/")
                .unwrap_or(local_shard_ref);
            let mut args = repository.global_args_for_exec();
            args.push("notes".to_string());
            args.push(format!("--ref={}", shard_name));
            args.push("merge".to_string());
            args.push("-s".to_string());
            args.push("ours".to_string());
            args.push("--quiet".to_string());
            args.push(tracking_ref.to_string());

            if let Err(e) = exec_git(&args) {
                debug_log(&format!(
                    "shard merge failed for {} <- {}: {}",
                    local_shard_ref, tracking_ref, e
                ));
            }
        } else {
            // Only tracking ref exists — copy it to local
            if let Err(e) = copy_ref(repository, tracking_ref, local_shard_ref) {
                debug_log(&format!(
                    "shard copy failed for {} <- {}: {}",
                    local_shard_ref, tracking_ref, e
                ));
            }
        }
    }
}

/// Result of checking for authorship notes on a remote
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotesExistence {
    /// Notes were found and fetched from the remote
    Found,
    /// Confirmed that no notes exist on the remote
    NotFound,
}

pub fn fetch_remote_from_args(
    repository: &Repository,
    parsed_args: &ParsedGitInvocation,
) -> Result<String, GitAiError> {
    let remotes = repository.remotes().ok();
    let remote_names: Vec<String> = remotes
        .as_ref()
        .map(|r| {
            (0..r.len())
                .filter_map(|i| r.get(i).map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // 2) Fetch authorship refs from the appropriate remote
    // Try to detect remote (named remote, URL, or local path) from args first
    let positional_remote = extract_remote_from_fetch_args(&parsed_args.command_args);
    let specified_remote = positional_remote.or_else(|| {
        parsed_args
            .command_args
            .iter()
            .find(|a| remote_names.iter().any(|r| r == *a))
            .cloned()
    });

    let remote = specified_remote
        .or_else(|| repository.upstream_remote().ok().flatten())
        .or_else(|| repository.get_default_remote().ok().flatten());

    remote.map(|r| r.to_string()).ok_or_else(|| {
        GitAiError::Generic(
            "Could not determine a remote for fetch/push operation. \
                 No remote was specified in args, no upstream is configured, \
                 and no default remote was found."
                .to_string(),
        )
    })
}

// for use with post-fetch and post-pull and post-clone hooks
// Returns Ok(NotesExistence::Found) if notes were found and fetched,
// Ok(NotesExistence::NotFound) if confirmed no notes exist on remote,
// Err(...) for actual errors (network, permissions, etc.)
pub fn fetch_authorship_notes(
    repository: &Repository,
    remote_name: &str,
) -> Result<NotesExistence, GitAiError> {
    // Generate tracking ref for this remote
    let tracking_ref = tracking_ref_for_remote(remote_name);

    debug_log(&format!(
        "fetching authorship notes for remote '{}' to tracking ref '{}'",
        remote_name, tracking_ref
    ));

    // Build refspecs: legacy + shard wildcard (when enabled)
    let fetch_refspec = format!("+refs/notes/ai:{}", tracking_ref);
    let mut refspecs = vec![fetch_refspec.as_str()];

    let shard_prefix = shard_tracking_ref_prefix(remote_name);
    let shard_refspec = format!("+{}*:{}*", AI_SHARDED_NOTES_PREFIX, shard_prefix);
    let sharded = sharded_notes_enabled();
    if sharded {
        refspecs.push(&shard_refspec);
    }

    let fetch_authorship =
        build_authorship_fetch_args(repository.global_args_for_exec(), remote_name, &refspecs);

    debug_log(&format!("fetch command: {:?}", fetch_authorship));

    match exec_git(&fetch_authorship) {
        Ok(output) => {
            debug_log(&format!(
                "fetch stdout: '{}'",
                String::from_utf8_lossy(&output.stdout)
            ));
            debug_log(&format!(
                "fetch stderr: '{}'",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Err(e) => {
            if is_missing_remote_notes_ref_error(&e) {
                debug_log(&format!(
                    "no authorship notes found on remote '{}', nothing to sync",
                    remote_name
                ));
                return Ok(NotesExistence::NotFound);
            }
            debug_log(&format!("authorship fetch failed: {}", e));
            return Err(e);
        }
    }

    // After successful fetch, merge the tracking ref into refs/notes/ai
    let local_notes_ref = "refs/notes/ai";

    if crate::git::refs::ref_exists(repository, &tracking_ref) {
        if crate::git::refs::ref_exists(repository, local_notes_ref) {
            debug_log(&format!(
                "merging authorship notes from {} into {}",
                tracking_ref, local_notes_ref
            ));
            if let Err(e) = merge_notes_from_ref(repository, &tracking_ref) {
                debug_log(&format!("notes merge failed: {}", e));
                // Fallback: manually merge notes when git notes merge crashes
                if let Err(e2) = fallback_merge_notes_ours(repository, &tracking_ref) {
                    debug_log(&format!("fallback merge also failed: {}", e2));
                }
            }
        } else {
            debug_log(&format!(
                "initializing {} from tracking ref {}",
                local_notes_ref, tracking_ref
            ));
            if let Err(e) = copy_ref(repository, &tracking_ref, local_notes_ref) {
                debug_log(&format!("notes copy failed: {}", e));
            }
        }
    } else {
        debug_log(&format!(
            "tracking ref {} was not created after fetch",
            tracking_ref
        ));
    }

    // Merge shard tracking refs when sharding is enabled
    if sharded {
        merge_shard_tracking_refs(repository, remote_name);
    }

    Ok(NotesExistence::Found)
}

fn is_missing_remote_notes_ref_error(error: &GitAiError) -> bool {
    let GitAiError::GitCliError { stderr, .. } = error else {
        return false;
    };

    let stderr_lower = stderr.to_ascii_lowercase();
    stderr_lower.contains("refs/notes/ai")
        && (stderr_lower.contains("couldn't find remote ref")
            || stderr_lower.contains("could not find remote ref")
            || stderr_lower.contains("remote ref does not exist")
            || stderr_lower.contains("not our ref"))
}
/// Maximum number of fetch-merge-push attempts before giving up.
/// On busy monorepos, concurrent pushers can cause non-fast-forward rejections
/// even after a successful merge, so we retry the full cycle.
const PUSH_NOTES_MAX_ATTEMPTS: usize = 3;

// for use with post-push hook
pub fn push_authorship_notes(repository: &Repository, remote_name: &str) -> Result<(), GitAiError> {
    let mut last_error = None;

    for attempt in 0..PUSH_NOTES_MAX_ATTEMPTS {
        if attempt > 0 {
            debug_log(&format!(
                "retrying notes push (attempt {}/{})",
                attempt + 1,
                PUSH_NOTES_MAX_ATTEMPTS
            ));
        }

        fetch_and_merge_tracking_notes(repository, remote_name);

        // Push notes without force (requires fast-forward)
        let sharded = sharded_notes_enabled();
        let shard_push_refspec =
            format!("{}*:{}*", AI_SHARDED_NOTES_PREFIX, AI_SHARDED_NOTES_PREFIX);
        let mut push_refspecs: Vec<&str> = vec![AI_AUTHORSHIP_PUSH_REFSPEC];
        if sharded {
            push_refspecs.push(&shard_push_refspec);
        }

        let push_args = build_authorship_push_args(
            repository.global_args_for_exec(),
            remote_name,
            &push_refspecs,
        );

        debug_log(&format!(
            "pushing authorship refs (no force): {:?}",
            &push_args
        ));

        match exec_git(&push_args) {
            Ok(_) => return Ok(()),
            Err(e) => {
                debug_log(&format!("authorship push failed: {}", e));
                if is_non_fast_forward_error(&e) && attempt + 1 < PUSH_NOTES_MAX_ATTEMPTS {
                    // Another pusher updated remote notes between our merge and push.
                    // Retry the full fetch-merge-push cycle.
                    last_error = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| GitAiError::Generic("notes push exhausted retries".to_string())))
}

/// Fetch remote notes into a tracking ref and merge into local refs/notes/ai.
fn fetch_and_merge_tracking_notes(repository: &Repository, remote_name: &str) {
    let sharded = sharded_notes_enabled();
    let tracking_ref = tracking_ref_for_remote(remote_name);
    let fetch_refspec = format!("+refs/notes/ai:{}", tracking_ref);
    let mut refspecs = vec![fetch_refspec.as_str()];

    let shard_prefix = shard_tracking_ref_prefix(remote_name);
    let shard_fetch_refspec = format!("+{}*:{}*", AI_SHARDED_NOTES_PREFIX, shard_prefix);
    if sharded {
        refspecs.push(&shard_fetch_refspec);
    }

    let fetch_args =
        build_authorship_fetch_args(repository.global_args_for_exec(), remote_name, &refspecs);

    debug_log(&format!("pre-push authorship fetch: {:?}", &fetch_args));

    // Fetch is best-effort; if it fails (e.g., no remote notes yet), continue
    if exec_git(&fetch_args).is_err() {
        return;
    }

    let local_notes_ref = "refs/notes/ai";

    if !ref_exists(repository, &tracking_ref) {
        return;
    }

    if !ref_exists(repository, local_notes_ref) {
        // Only tracking ref exists - copy it to local
        debug_log(&format!(
            "pre-push: initializing {} from {}",
            local_notes_ref, tracking_ref
        ));
        if let Err(e) = copy_ref(repository, &tracking_ref, local_notes_ref) {
            debug_log(&format!("pre-push notes copy failed: {}", e));
        }
        return;
    }

    // Both exist - merge them
    debug_log(&format!(
        "pre-push: merging {} into {}",
        tracking_ref, local_notes_ref
    ));
    if let Err(e) = merge_notes_from_ref(repository, &tracking_ref) {
        debug_log(&format!("pre-push notes merge failed: {}", e));
        // Fallback: manually merge notes when git notes merge crashes
        // (e.g., due to corrupted/mixed-fanout notes trees, or git bugs
        // with fanout-level mismatches on older git versions like macOS)
        if let Err(e2) = fallback_merge_notes_ours(repository, &tracking_ref) {
            debug_log(&format!("pre-push fallback merge also failed: {}", e2));
        }
    }

    // Merge shard tracking refs
    if sharded {
        merge_shard_tracking_refs(repository, remote_name);
    }
}

fn is_non_fast_forward_error(error: &GitAiError) -> bool {
    let GitAiError::GitCliError { stderr, .. } = error else {
        return false;
    };
    stderr.contains("non-fast-forward")
}

fn extract_remote_from_fetch_args(args: &[String]) -> Option<String> {
    let mut after_double_dash = false;

    for arg in args {
        if !after_double_dash {
            if arg == "--" {
                after_double_dash = true;
                continue;
            }
            if arg.starts_with('-') {
                // Option; skip
                continue;
            }
        }

        // Candidate positional arg; determine if it's a repository URL/path
        let s = arg.as_str();

        // 1) URL forms (https://, ssh://, file://, git://, etc.)
        if s.contains("://") || s.starts_with("file://") {
            return Some(arg.clone());
        }

        // 2) SCP-like syntax: user@host:path
        if s.contains('@') && s.contains(':') && !s.contains("://") {
            return Some(arg.clone());
        }

        // 3) Local path forms
        if s.starts_with('/') || s.starts_with("./") || s.starts_with("../") || s.starts_with("~/")
        {
            return Some(arg.clone());
        }

        // Heuristic: bare repo directories often end with .git
        if s.ends_with(".git") {
            return Some(arg.clone());
        }

        // 4) As a last resort, if the path exists on disk, treat as local path
        if std::path::Path::new(s).exists() {
            return Some(arg.clone());
        }

        // Otherwise, do not treat this positional token as a repository; likely a refspec
        break;
    }

    None
}

fn with_disabled_hooks(mut args: Vec<String>) -> Vec<String> {
    args.push("-c".to_string());
    args.push(disabled_hooks_config().to_string());
    args
}

fn build_authorship_fetch_args(
    global_args: Vec<String>,
    remote_name: &str,
    fetch_refspecs: &[&str],
) -> Vec<String> {
    let mut args = with_disabled_hooks(global_args);
    args.push("fetch".to_string());
    args.push("--no-tags".to_string());
    args.push("--recurse-submodules=no".to_string());
    args.push("--no-write-fetch-head".to_string());
    args.push("--no-write-commit-graph".to_string());
    args.push("--no-auto-maintenance".to_string());
    args.push(remote_name.to_string());
    for refspec in fetch_refspecs {
        args.push(refspec.to_string());
    }
    args
}

fn build_authorship_push_args(
    global_args: Vec<String>,
    remote_name: &str,
    push_refspecs: &[&str],
) -> Vec<String> {
    let mut args = with_disabled_hooks(global_args);
    args.push("push".to_string());
    args.push("--quiet".to_string());
    args.push("--no-recurse-submodules".to_string());
    args.push("--no-verify".to_string());
    args.push("--no-signed".to_string());
    args.push(remote_name.to_string());
    for refspec in push_refspecs {
        args.push(refspec.to_string());
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorship_fetch_args_always_disable_hooks() {
        let disabled_hooks = disabled_hooks_config();
        let args = build_authorship_fetch_args(
            vec!["-C".to_string(), "/tmp/repo".to_string()],
            "origin",
            &["+refs/notes/ai:refs/notes/ai-remote/origin"],
        );

        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-c" && pair[1] == disabled_hooks)
        );
        assert!(args.contains(&"fetch".to_string()));
    }

    #[test]
    fn authorship_push_args_always_disable_hooks() {
        let disabled_hooks = disabled_hooks_config();
        let args = build_authorship_push_args(
            vec!["-C".to_string(), "/tmp/repo".to_string()],
            "origin",
            &[AI_AUTHORSHIP_PUSH_REFSPEC],
        );

        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-c" && pair[1] == disabled_hooks)
        );
        assert!(args.contains(&"push".to_string()));
    }

    #[test]
    fn missing_remote_notes_ref_error_is_detected() {
        let err = GitAiError::GitCliError {
            code: Some(128),
            stderr: "fatal: couldn't find remote ref refs/notes/ai".to_string(),
            args: vec!["fetch".to_string(), "origin".to_string()],
        };
        assert!(is_missing_remote_notes_ref_error(&err));
    }

    #[test]
    fn missing_remote_notes_ref_error_ignores_unrelated_git_errors() {
        let err = GitAiError::GitCliError {
            code: Some(128),
            stderr: "fatal: Authentication failed for 'https://github.com/org/repo.git/'"
                .to_string(),
            args: vec!["fetch".to_string(), "origin".to_string()],
        };
        assert!(!is_missing_remote_notes_ref_error(&err));
    }
}
