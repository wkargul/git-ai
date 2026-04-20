//! Lightweight aggregated performance instrumentation for diagnosing Windows slowness.
//!
//! # How to enable
//!
//! Set `GIT_AI_DEBUG_PERFORMANCE=1` in the environment before running any `git` or
//! `git-ai` command.  A compact report is printed to stderr when the process exits.
//!
//! # What is measured
//!
//! | Label prefix | Source |
//! |---|---|
//! | `git.*`      | Every internal `exec_git*` subprocess call, bucketed by subcommand |
//! | `sqlite.*`   | Key `InternalDatabase` operations (open, upsert, batch, read) |
//! | `test.*`     | Test-infrastructure setup/teardown timings (test binary only) |
//!
//! # Example output
//!
//! ```text
//! [git-ai perf] ────────────────────────────────────────────────────────────────────────
//! [git-ai perf]  label                                   count     total      avg      max
//! [git-ai perf]  git.rev-parse                              47   3,210ms    68ms    142ms
//! [git-ai perf]  git.notes                                  23   1,840ms    80ms    156ms
//! [git-ai perf]  git.fast-import                             5     280ms    56ms     89ms
//! [git-ai perf]  git.cat-file                                4     180ms    45ms     67ms
//! [git-ai perf]  git.show                                   12     420ms    35ms     72ms
//! [git-ai perf]  git.merge-base                              6     190ms    32ms     58ms
//! [git-ai perf]  sqlite.open                                 1      28ms    28ms     28ms
//! [git-ai perf]  sqlite.upsert_prompt                        3      12ms     4ms      8ms
//! [git-ai perf]  sqlite.batch_upsert_prompts                 1       8ms     8ms      8ms
//! [git-ai perf]  sqlite.get_prompt                           2       6ms     3ms      4ms
//! [git-ai perf] ────────────────────────────────────────────────────────────────────────
//! ```
//!
//! # Design
//!
//! - Zero overhead when disabled: `is_enabled()` resolves to a single cached `bool` read
//!   after the first call.
//! - Thread-safe: a global `Mutex<HashMap<&'static str, Bucket>>` accumulates counters from
//!   all threads.  Contention is negligible because lock is held only for the short
//!   arithmetic update, not during the measured operation itself.
//! - Auto-report: `libc::atexit` registers the print callback once when the env var is
//!   first observed, so the report is emitted even when the process exits via
//!   `std::process::exit()`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

// ── enabled flag ──────────────────────────────────────────────────────────────

static ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns `true` when `GIT_AI_DEBUG_PERFORMANCE` is set to a non-empty, non-`"0"` value.
///
/// The result is cached after the first call; subsequent calls are a single
/// atomic load with no heap allocation.
pub fn is_enabled() -> bool {
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("GIT_AI_DEBUG_PERFORMANCE")
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false);
        if enabled {
            register_atexit();
        }
        enabled
    })
}

// ── per-label accumulator ─────────────────────────────────────────────────────

#[derive(Default)]
struct Bucket {
    count: u64,
    /// Saturating sum so an absurdly long operation cannot overflow.
    total_ns: u64,
    max_ns: u64,
}

static STORE: OnceLock<Mutex<HashMap<&'static str, Bucket>>> = OnceLock::new();

fn store() -> &'static Mutex<HashMap<&'static str, Bucket>> {
    STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

// ── public API ────────────────────────────────────────────────────────────────

/// Time a closure and accumulate the result under `label`.
///
/// Returns the closure's return value unchanged.  When instrumentation is
/// disabled this is a direct, zero-overhead call to `f()`.
///
/// ```no_run
/// # use git_ai::observability::perf;
/// let output = perf::measure("git.rev-parse", || do_rev_parse());
/// ```
pub fn measure<F, R>(label: &'static str, f: F) -> R
where
    F: FnOnce() -> R,
{
    if !is_enabled() {
        return f();
    }
    let start = Instant::now();
    let result = f();
    record_elapsed(label, start.elapsed());
    result
}

/// Record a pre-measured duration under `label`.
///
/// Use this when the code under measurement cannot be wrapped in a closure
/// (e.g. functions with complex early-return paths where [`Timer`] is also
/// unsuitable).
pub fn record(label: &'static str, elapsed: Duration) {
    if !is_enabled() {
        return;
    }
    record_elapsed(label, elapsed);
}

/// Internal helper — writes into STORE without re-checking `is_enabled`.
#[inline]
fn record_elapsed(label: &'static str, elapsed: Duration) {
    let elapsed_ns = elapsed.as_nanos() as u64;
    if let Ok(mut guard) = store().lock() {
        let bucket = guard.entry(label).or_insert_with(Bucket::default);
        bucket.count += 1;
        bucket.total_ns = bucket.total_ns.saturating_add(elapsed_ns);
        if elapsed_ns > bucket.max_ns {
            bucket.max_ns = elapsed_ns;
        }
    }
}

/// RAII timer that records elapsed time when dropped.
///
/// Prefer this over [`measure`] for functions that have multiple early-return
/// points (e.g. those using the `?` operator extensively), where wrapping the
/// entire body in a closure is awkward.
///
/// ```no_run
/// # use git_ai::observability::perf::Timer;
/// fn do_work() -> std::io::Result<()> {
///     let _t = Timer::start("sqlite.open");  // records on drop, even via `?`
///     let f = std::fs::File::open("db")?;
///     Ok(())
/// }
/// ```
#[must_use = "drop the timer when the measured section ends; a discarded Timer records immediately"]
pub struct Timer {
    label: &'static str,
    start: Instant,
    /// `false` when instrumentation is disabled so `Drop` is a no-op.
    active: bool,
}

impl Timer {
    /// Start a new timer.  When instrumentation is disabled, this is a no-op
    /// struct creation with no `Instant::now()` call.
    pub fn start(label: &'static str) -> Self {
        let active = is_enabled();
        Self {
            label,
            // Initialise even when inactive so the struct is always valid.
            // The cost is a single syscall-free integer read on modern OSes.
            start: Instant::now(),
            active,
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if self.active {
            record_elapsed(self.label, self.start.elapsed());
        }
    }
}

// ── git subcommand label extraction ──────────────────────────────────────────

/// Extract a `&'static str` label for the git subcommand embedded in `args`.
///
/// Global git flags that precede the subcommand (e.g. `-C <path>`, `-c key=val`,
/// `--git-dir <dir>`) are correctly skipped.
///
/// Unknown subcommands map to `"git.other"` rather than leaking arbitrary
/// strings into the `&'static str` HashMap.
pub fn git_subcommand_label(args: &[String]) -> &'static str {
    let mut i = 0usize;
    while i < args.len() {
        let arg = args[i].as_str();
        if !arg.starts_with('-') {
            return intern_subcommand(arg);
        }
        // These flags each consume the following argument as their value.
        if matches!(
            arg,
            "-C" | "-c"
                | "--git-dir"
                | "--work-tree"
                | "--namespace"
                | "--super-prefix"
                | "--config-env"
        ) {
            i += 2;
        } else {
            i += 1;
        }
    }
    "git.unknown"
}

fn intern_subcommand(s: &str) -> &'static str {
    match s {
        "add" => "git.add",
        "blame" => "git.blame",
        "cat-file" => "git.cat-file",
        "check-attr" => "git.check-attr",
        "cherry-pick" => "git.cherry-pick",
        "clone" => "git.clone",
        "commit" => "git.commit",
        "commit-tree" => "git.commit-tree",
        "diff" => "git.diff",
        "diff-tree" => "git.diff-tree",
        "fast-import" => "git.fast-import",
        "checkout" => "git.checkout",
        "config" => "git.config",
        "fetch" => "git.fetch",
        "for-each-ref" => "git.for-each-ref",
        "hash-object" => "git.hash-object",
        "init" => "git.init",
        "log" => "git.log",
        "ls-files" => "git.ls-files",
        "ls-tree" => "git.ls-tree",
        "merge" => "git.merge",
        "merge-base" => "git.merge-base",
        "notes" => "git.notes",
        "push" => "git.push",
        "rebase" => "git.rebase",
        "reflog" => "git.reflog",
        "reset" => "git.reset",
        "rev-list" => "git.rev-list",
        "rev-parse" => "git.rev-parse",
        "show" => "git.show",
        "remote" => "git.remote",
        "stash" => "git.stash",
        "status" => "git.status",
        "switch" => "git.switch",
        "symbolic-ref" => "git.symbolic-ref",
        "update-ref" => "git.update-ref",
        "worktree" => "git.worktree",
        _ => "git.other",
    }
}

// ── report ────────────────────────────────────────────────────────────────────

/// Print the aggregated report to stderr, sorted by total time descending.
///
/// Only emits output when `GIT_AI_DEBUG_PERFORMANCE=1`.  Safe to call multiple
/// times; subsequent calls after the data is already printed will re-print
/// whatever is currently in the store.
pub fn print_report() {
    if !is_enabled() {
        return;
    }
    let Ok(guard) = store().lock() else {
        return;
    };
    if guard.is_empty() {
        return;
    }

    // Collect and sort by total_ns descending, then label ascending for
    // deterministic output when two labels have the same total.
    let mut rows: Vec<(&'static str, u64, u64, u64)> = guard
        .iter()
        .map(|(&label, b)| (label, b.count, b.total_ns, b.max_ns))
        .collect();
    rows.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(b.0)));

    let sep = "─".repeat(80);
    eprintln!("[git-ai perf] {sep}");
    eprintln!(
        "[git-ai perf]  {:<42} {:>5}  {:>8}  {:>7}  {:>7}",
        "label", "count", "total", "avg", "max"
    );
    for (label, count, total_ns, max_ns) in &rows {
        let avg_ns = if *count > 0 { total_ns / count } else { 0 };
        eprintln!(
            "[git-ai perf]  {:<42} {:>5}  {:>6}ms  {:>5}ms  {:>5}ms",
            label,
            count,
            total_ns / 1_000_000,
            avg_ns / 1_000_000,
            max_ns / 1_000_000,
        );
    }
    eprintln!("[git-ai perf] {sep}");
}

// ── atexit registration ───────────────────────────────────────────────────────

/// Register `print_report` to run when the process exits, including via
/// `std::process::exit()`.
///
/// Called at most once (guarded by an `AtomicBool`).  Uses `libc::atexit`
/// which is available on Linux, macOS, and Windows (MSVCRT).
fn register_atexit() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static REGISTERED: AtomicBool = AtomicBool::new(false);
    if REGISTERED.swap(true, Ordering::SeqCst) {
        return; // already registered — nothing to do
    }

    extern "C" fn on_exit() {
        print_report();
    }

    // SAFETY:
    //   - `on_exit` is a plain `extern "C"` function pointer with no captures
    //     and no references to Rust objects that might have been destroyed.
    //   - `libc::atexit` is a standard C function available on all three
    //     target platforms (Linux / macOS / Windows-MSVCRT).
    //   - The `STORE` OnceLock and its Mutex are not destroyed before atexit
    //     callbacks run (static lifetime), so `print_report` can safely
    //     acquire the lock.  If the Mutex is poisoned the function returns
    //     early via `let Ok(...) else { return; }`.
    unsafe {
        libc::atexit(on_exit);
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_subcommand_label_simple() {
        let args = vec!["rev-parse".to_string(), "--verify".to_string(), "HEAD".to_string()];
        assert_eq!(git_subcommand_label(&args), "git.rev-parse");
    }

    #[test]
    fn test_git_subcommand_label_skips_dash_c() {
        // -c key=val precedes the subcommand in internal exec calls
        let args = vec![
            "-c".to_string(),
            "core.hooksPath=/dev/null".to_string(),
            "notes".to_string(),
            "--ref=ai".to_string(),
            "show".to_string(),
        ];
        assert_eq!(git_subcommand_label(&args), "git.notes");
    }

    #[test]
    fn test_git_subcommand_label_skips_dash_capital_c() {
        let args = vec![
            "-C".to_string(),
            "/some/path".to_string(),
            "status".to_string(),
        ];
        assert_eq!(git_subcommand_label(&args), "git.status");
    }

    #[test]
    fn test_git_subcommand_label_skips_git_dir() {
        let args = vec![
            "--git-dir".to_string(),
            "/repo/.git".to_string(),
            "cat-file".to_string(),
            "--batch".to_string(),
        ];
        assert_eq!(git_subcommand_label(&args), "git.cat-file");
    }

    #[test]
    fn test_git_subcommand_label_unknown() {
        let args = vec!["frobnicate".to_string()];
        assert_eq!(git_subcommand_label(&args), "git.other");
    }

    #[test]
    fn test_git_subcommand_label_empty() {
        let args: Vec<String> = vec![];
        assert_eq!(git_subcommand_label(&args), "git.unknown");
    }

    #[test]
    fn test_git_subcommand_label_multiple_global_flags() {
        // -C path -c key=val <subcommand>
        let args = vec![
            "-C".to_string(),
            "/tmp/repo".to_string(),
            "-c".to_string(),
            "core.hooksPath=NUL".to_string(),
            "fast-import".to_string(),
            "--quiet".to_string(),
        ];
        assert_eq!(git_subcommand_label(&args), "git.fast-import");
    }
}
