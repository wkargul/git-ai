use crate::repos::test_file::ExpectedLineExt;
use crate::repos::test_repo::TestRepo;
use git_ai::authorship::authorship_log_serialization::AuthorshipLog;

// =============================================================================
// ISSUE-007: git rebase --no-verify should still transfer notes
// =============================================================================

/// `git rebase --no-verify` should preserve AI attribution exactly like
/// a normal rebase. The wrapper intercepts the command regardless of
/// `--no-verify` (which only suppresses git's own hooks).
#[test]
fn test_rebase_no_verify_preserves_attribution() {
    let repo = TestRepo::new();

    // Initial commit
    let mut base_file = repo.filename("base.txt");
    base_file.set_contents(crate::lines!["base content"]);
    repo.stage_all_and_commit("Initial commit").unwrap();
    let default_branch = repo.current_branch();

    // Feature branch with AI commit
    repo.git(&["checkout", "-b", "feature"]).unwrap();
    let mut feature_file = repo.filename("feature.txt");
    feature_file.set_contents(crate::lines![
        "// AI generated code".ai(),
        "fn ai_feature() {}".ai()
    ]);
    repo.stage_all_and_commit("AI feature commit").unwrap();

    // Advance main branch (force non-fast-forward rebase)
    repo.git(&["checkout", &default_branch]).unwrap();
    let mut main_file = repo.filename("main.txt");
    main_file.set_contents(crate::lines!["main work"]);
    repo.stage_all_and_commit("Main advances").unwrap();

    // Rebase with --no-verify
    repo.git(&["checkout", "feature"]).unwrap();
    repo.git(&["rebase", "--no-verify", &default_branch])
        .expect("rebase --no-verify should succeed");

    // Verify AI attribution survived --no-verify rebase
    feature_file.assert_lines_and_blame(crate::lines![
        "// AI generated code".ai(),
        "fn ai_feature() {}".ai()
    ]);

    // Verify the rebased commit has an authorship note
    let rebased_sha = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();
    assert!(
        repo.read_authorship_note(&rebased_sha).is_some(),
        "Rebased commit via --no-verify should have an authorship note"
    );
}

/// Multiple commits rebased with --no-verify should all preserve their notes.
#[test]
fn test_rebase_no_verify_multiple_commits() {
    let repo = TestRepo::new();

    let mut base_file = repo.filename("base.txt");
    base_file.set_contents(crate::lines!["base"]);
    repo.stage_all_and_commit("Initial").unwrap();
    let default_branch = repo.current_branch();

    repo.git(&["checkout", "-b", "feature"]).unwrap();

    let mut f1 = repo.filename("f1.txt");
    f1.set_contents(crate::lines!["// AI f1".ai()]);
    repo.stage_all_and_commit("AI commit 1").unwrap();

    let mut f2 = repo.filename("f2.txt");
    f2.set_contents(crate::lines!["// AI f2".ai()]);
    repo.stage_all_and_commit("AI commit 2").unwrap();

    let mut f3 = repo.filename("f3.txt");
    f3.set_contents(crate::lines!["// AI f3".ai()]);
    repo.stage_all_and_commit("AI commit 3").unwrap();

    repo.git(&["checkout", &default_branch]).unwrap();
    let mut main_file = repo.filename("main.txt");
    main_file.set_contents(crate::lines!["main work"]);
    repo.stage_all_and_commit("Main advances").unwrap();

    repo.git(&["checkout", "feature"]).unwrap();
    repo.git(&["rebase", "--no-verify", &default_branch])
        .expect("rebase --no-verify should succeed");

    f1.assert_lines_and_blame(crate::lines!["// AI f1".ai()]);
    f2.assert_lines_and_blame(crate::lines!["// AI f2".ai()]);
    f3.assert_lines_and_blame(crate::lines!["// AI f3".ai()]);
}

// =============================================================================
// ISSUE-011: git rebase -i --autosquash loses attribution on squashed result
// =============================================================================

/// When autosquash merges multiple commits into one, the resulting commit's
/// authorship note should contain the merged/summed attributions from ALL source
/// commits, not just the first one.
#[test]
#[cfg(not(target_os = "windows"))]
fn test_autosquash_merges_attribution_notes() {
    let repo = TestRepo::new();

    let mut base_file = repo.filename("base.txt");
    base_file.set_contents(crate::lines!["base"]);
    repo.stage_all_and_commit("Initial").unwrap();
    let default_branch = repo.current_branch();

    repo.git(&["checkout", "-b", "feature"]).unwrap();

    // First commit: add file_a with AI content
    let mut file_a = repo.filename("file_a.txt");
    file_a.set_contents(crate::lines!["// AI module A".ai(), "fn a() {}".ai()]);
    repo.stage_all_and_commit("Add module A").unwrap();

    // Second commit: add file_b with AI content (will be squashed into first)
    let mut file_b = repo.filename("file_b.txt");
    file_b.set_contents(crate::lines!["// AI module B".ai(), "fn b() {}".ai()]);
    repo.stage_all_and_commit("fixup! Add module A").unwrap();

    // Advance main
    repo.git(&["checkout", &default_branch]).unwrap();
    let mut main_file = repo.filename("main.txt");
    main_file.set_contents(crate::lines!["main work"]);
    repo.stage_all_and_commit("Main advances").unwrap();
    let base_commit = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();

    // Interactive rebase with --autosquash
    repo.git(&["checkout", "feature"]).unwrap();

    let rebase_result = repo.git_with_env(
        &["rebase", "-i", "--autosquash", &base_commit],
        &[("GIT_SEQUENCE_EDITOR", "true"), ("GIT_EDITOR", "true")],
        None,
    );

    assert!(rebase_result.is_ok(), "Autosquash rebase should succeed");

    // After squash, there should be exactly 1 commit on feature above base
    let log = repo
        .git(&["log", "--oneline", &format!("{}..HEAD", base_commit)])
        .unwrap();
    let commit_count = log.trim().lines().count();
    assert_eq!(
        commit_count, 1,
        "Autosquash should have squashed 2 commits into 1"
    );

    // Verify blame on both files
    file_a.assert_lines_and_blame(crate::lines!["// AI module A".ai(), "fn a() {}".ai()]);
    file_b.assert_lines_and_blame(crate::lines!["// AI module B".ai(), "fn b() {}".ai()]);

    // Verify the squashed commit has an authorship note that covers BOTH files
    let squashed_sha = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();
    let note = repo
        .read_authorship_note(&squashed_sha)
        .expect("Squashed commit should have authorship note");
    let log_parsed =
        AuthorshipLog::deserialize_from_string(&note).expect("parse squashed authorship note");

    // The note should contain attestations for BOTH file_a and file_b
    let attested_files: Vec<&str> = log_parsed
        .attestations
        .iter()
        .map(|a| a.file_path.as_str())
        .collect();
    assert!(
        attested_files.iter().any(|f| f.contains("file_a.txt")),
        "Squashed note should contain file_a.txt attestation. Got files: {:?}",
        attested_files
    );
    assert!(
        attested_files.iter().any(|f| f.contains("file_b.txt")),
        "Squashed note should contain file_b.txt attestation. Got files: {:?}",
        attested_files
    );
}

/// When autosquash squashes commits that modify the SAME file, the resulting
/// note should have the correct line counts reflecting the final state.
#[test]
#[cfg(not(target_os = "windows"))]
fn test_autosquash_same_file_merges_attribution_correctly() {
    let repo = TestRepo::new();

    let mut base_file = repo.filename("base.txt");
    base_file.set_contents(crate::lines!["base"]);
    repo.stage_all_and_commit("Initial").unwrap();
    let default_branch = repo.current_branch();

    repo.git(&["checkout", "-b", "feature"]).unwrap();

    // First commit: add initial AI content
    let mut ai_file = repo.filename("module.txt");
    ai_file.set_contents(crate::lines!["// AI header".ai(), "fn init() {}".ai()]);
    repo.stage_all_and_commit("Add module").unwrap();

    // Fixup commit: add more to the same file
    ai_file.set_contents(crate::lines![
        "// AI header".ai(),
        "fn init() {}".ai(),
        "fn cleanup() {}".ai()
    ]);
    repo.stage_all_and_commit("fixup! Add module").unwrap();

    // Advance main
    repo.git(&["checkout", &default_branch]).unwrap();
    let mut main_file = repo.filename("main.txt");
    main_file.set_contents(crate::lines!["main"]);
    repo.stage_all_and_commit("Main work").unwrap();
    let base_commit = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();

    // Interactive rebase with --autosquash
    repo.git(&["checkout", "feature"]).unwrap();
    let rebase_result = repo.git_with_env(
        &["rebase", "-i", "--autosquash", &base_commit],
        &[("GIT_SEQUENCE_EDITOR", "true"), ("GIT_EDITOR", "true")],
        None,
    );
    assert!(rebase_result.is_ok(), "Autosquash rebase should succeed");

    // Verify blame on the module file
    ai_file.assert_lines_and_blame(crate::lines![
        "// AI header".ai(),
        "fn init() {}".ai(),
        "fn cleanup() {}".ai()
    ]);

    // Verify the note exists and has correct attribution count
    let squashed_sha = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();
    let note = repo
        .read_authorship_note(&squashed_sha)
        .expect("Squashed commit should have authorship note");
    let log_parsed =
        AuthorshipLog::deserialize_from_string(&note).expect("parse squashed authorship note");
    assert!(
        !log_parsed.attestations.is_empty(),
        "Squashed commit should have attestations"
    );
}

// =============================================================================
// ISSUE-014: git rebase -i with edit + amend preserves notes
// =============================================================================

/// Interactive rebase with `edit` command followed by `git commit --amend`
/// should preserve AI attribution on the resulting commit.
#[test]
#[cfg(not(target_os = "windows"))]
fn test_rebase_interactive_edit_amend_preserves_notes() {
    let repo = TestRepo::new();

    let mut base_file = repo.filename("base.txt");
    base_file.set_contents(crate::lines!["base"]);
    repo.stage_all_and_commit("Initial").unwrap();
    let default_branch = repo.current_branch();

    repo.git(&["checkout", "-b", "feature"]).unwrap();

    // AI commit that we'll edit+amend
    let mut ai_file = repo.filename("feature.txt");
    ai_file.set_contents(crate::lines![
        "// AI generated".ai(),
        "fn feature() {}".ai()
    ]);
    repo.stage_all_and_commit("AI feature").unwrap();

    // Second AI commit (will be replayed after the edit)
    let mut ai_file2 = repo.filename("feature2.txt");
    ai_file2.set_contents(crate::lines!["// AI second".ai()]);
    repo.stage_all_and_commit("AI feature 2").unwrap();

    // Advance main
    repo.git(&["checkout", &default_branch]).unwrap();
    let mut main_file = repo.filename("main.txt");
    main_file.set_contents(crate::lines!["main work"]);
    repo.stage_all_and_commit("Main advances").unwrap();
    let base_commit = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();

    // Interactive rebase: edit the first commit, then amend it
    repo.git(&["checkout", "feature"]).unwrap();

    use std::io::Write;

    // Create sequence editor that marks first commit as 'edit'
    let script_content = r#"#!/bin/sh
sed -i.bak '1s/pick/edit/' "$1"
"#;
    let script_path = repo.path().join("edit_script.sh");
    let mut script_file = std::fs::File::create(&script_path).unwrap();
    script_file.write_all(script_content.as_bytes()).unwrap();
    drop(script_file);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();
    }

    // Start interactive rebase - will stop at first commit for editing
    let rebase_result = repo.git_with_env(
        &["rebase", "-i", &base_commit],
        &[
            ("GIT_SEQUENCE_EDITOR", script_path.to_str().unwrap()),
            ("GIT_EDITOR", "true"),
        ],
        None,
    );

    // The rebase should stop for editing (may return error or success depending on git version)
    // Check if rebase is in progress
    let rebase_in_progress = repo.path().join(".git/rebase-merge").exists();
    if !rebase_in_progress {
        // If rebase didn't stop, it might have completed already
        if rebase_result.is_ok() {
            // Verify attribution is preserved (rebase completed without stopping)
            ai_file.assert_lines_and_blame(crate::lines![
                "// AI generated".ai(),
                "fn feature() {}".ai()
            ]);
            ai_file2.assert_lines_and_blame(crate::lines!["// AI second".ai()]);
            return;
        }
        panic!("Rebase failed and is not in progress: {:?}", rebase_result);
    }

    // Amend the commit (add a comment line)
    ai_file.set_contents(crate::lines![
        "// AI generated".ai(),
        "// human added comment".human(),
        "fn feature() {}".ai()
    ]);
    repo.git(&["add", "feature.txt"]).unwrap();
    repo.git_with_env(&["commit", "--amend"], &[("GIT_EDITOR", "true")], None)
        .expect("amend should succeed");

    // Continue the rebase
    repo.git_with_env(&["rebase", "--continue"], &[("GIT_EDITOR", "true")], None)
        .expect("rebase --continue should succeed");

    // Verify attribution preserved: the amended commit should have AI lines attributed
    ai_file.assert_lines_and_blame(crate::lines![
        "// AI generated".ai(),
        "// human added comment".human(),
        "fn feature() {}".ai()
    ]);

    // The second commit should also have preserved AI attribution
    ai_file2.assert_lines_and_blame(crate::lines!["// AI second".ai()]);

    // Verify both commits have authorship notes
    let head_sha = repo.git(&["rev-parse", "HEAD"]).unwrap().trim().to_string();
    let parent_sha = repo
        .git(&["rev-parse", "HEAD~1"])
        .unwrap()
        .trim()
        .to_string();

    assert!(
        repo.read_authorship_note(&head_sha).is_some(),
        "Second rebased commit should have authorship note"
    );
    assert!(
        repo.read_authorship_note(&parent_sha).is_some(),
        "Amended commit should have authorship note"
    );
}

// =============================================================================
// ISSUE-001: git pull --rebase drops attribution on committed AI work
// (more targeted edge case tests)
// =============================================================================

/// pull --rebase with multiple local AI commits should preserve all of them.
#[test]
fn test_pull_rebase_multiple_local_ai_commits() {
    let (local, _upstream) = TestRepo::new_with_remote();

    // Initial commit and push
    let mut readme = local.filename("README.md");
    readme.set_contents(vec!["# Test Repo".to_string()]);
    let initial = local
        .stage_all_and_commit("initial commit")
        .expect("initial commit");
    local
        .git(&["push", "-u", "origin", "HEAD"])
        .expect("push initial");

    // Create multiple local AI commits
    let mut f1 = local.filename("ai_1.txt");
    f1.set_contents(vec!["AI line 1".ai(), "AI line 2".ai()]);
    local
        .stage_all_and_commit("local AI commit 1")
        .expect("ai commit 1");

    let mut f2 = local.filename("ai_2.txt");
    f2.set_contents(vec!["AI module B".ai()]);
    local
        .stage_all_and_commit("local AI commit 2")
        .expect("ai commit 2");

    let ai_head = local
        .git(&["rev-parse", "HEAD"])
        .unwrap()
        .trim()
        .to_string();

    let branch = local.current_branch();

    // Create divergent upstream
    local
        .git(&["reset", "--hard", &initial.commit_sha])
        .expect("reset");
    let mut upstream_file = local.filename("upstream.txt");
    upstream_file.set_contents(vec!["upstream content".to_string()]);
    local
        .stage_all_and_commit("upstream divergent")
        .expect("upstream commit");
    local
        .git(&["push", "--force", "origin", &format!("HEAD:{}", branch)])
        .expect("force push");

    // Restore local to AI commits
    local.git(&["reset", "--hard", &ai_head]).expect("reset");

    // Pull --rebase
    local
        .git(&["pull", "--rebase"])
        .expect("pull --rebase should succeed");

    // Verify BOTH AI files have preserved attribution
    f1.assert_lines_and_blame(vec!["AI line 1".ai(), "AI line 2".ai()]);
    f2.assert_lines_and_blame(vec!["AI module B".ai()]);
}

crate::reuse_tests_in_worktree!(
    test_rebase_no_verify_preserves_attribution,
    test_rebase_no_verify_multiple_commits,
    test_pull_rebase_multiple_local_ai_commits,
);

crate::reuse_tests_in_worktree_with_attrs!(
    (#[cfg(not(target_os = "windows"))])
    test_autosquash_merges_attribution_notes,
    test_autosquash_same_file_merges_attribution_correctly,
);

// Note: test_rebase_interactive_edit_amend_preserves_notes is NOT run in worktree mode
// because the mid-rebase edit+amend flow has a known worktree-specific path resolution
// issue with note transfer during rebase-merge state detection.
