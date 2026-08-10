//! CI must execute the runtime Moon actually ships.
//!
//! Moon's default feature set is `runtime-monoio`, and that is what ships on
//! Linux. Yet every CI job that EXECUTED tests did so under
//! `--no-default-features --features runtime-tokio,…` — 26 monoio integration
//! test files and 30 monoio-gated `src/` files were unreachable by CI. That is
//! how the v0.8.6 inline-GET ACL bypass (#457) shipped green: it was wrong only
//! on the monoio path, and no CI job could see it.
//!
//! These tests guard the FIX, not the bug. They read `.github/workflows/ci.yml`
//! and assert the monoio job exists and has not been quietly weakened — because
//! the failure mode of CI coverage is silent: a job that stops running, or is
//! switched to the wrong feature set, looks identical to a green build.
//!
//! Deliberately a repo-config test, not a runtime test: nothing observable at
//! runtime can tell you which runtime CI exercised.

use std::path::PathBuf;

fn workflow() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".github/workflows/ci.yml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Return the body of a top-level job block, from `  <name>:` to the next
/// top-level job key at the same two-space indent.
fn job_block(yaml: &str, name: &str) -> Option<String> {
    let needle = format!("\n  {name}:\n");
    let start = yaml.find(&needle)? + 1;
    let rest = &yaml[start + needle.len() - 1..];
    // Next line that begins with exactly two spaces then a non-space: the next job.
    let mut end = rest.len();
    for (idx, _) in rest.match_indices('\n') {
        let line = &rest[idx + 1..];
        let after_indent = line.strip_prefix("  ");
        if let Some(a) = after_indent
            && !a.starts_with(' ')
            && !a.starts_with('#')
            && a.contains(':')
        {
            end = idx + 1;
            break;
        }
    }
    Some(rest[..end].to_string())
}

#[test]
fn ci_has_a_job_that_tests_the_default_monoio_runtime() {
    let yaml = workflow();
    let job = job_block(&yaml, "check-monoio").expect(
        "ci.yml has no `check-monoio` job. CI would then execute tests ONLY under \
         runtime-tokio, leaving the shipped runtime untested — the gap that let the \
         v0.8.6 inline-GET ACL bypass (#457) ship green.",
    );

    let tests = job
        .lines()
        .filter(|l| l.contains("cargo nextest run") || l.contains("cargo test"))
        .collect::<Vec<_>>();
    assert!(
        !tests.is_empty(),
        "`check-monoio` runs no test command at all; it would be theatre.\n{job}"
    );

    for line in &tests {
        assert!(
            !line.contains("--no-default-features"),
            "`check-monoio` must test the DEFAULT (monoio) feature set. This line opts out \
             of it, which silently makes the job a duplicate of the tokio `check` job:\n  {line}"
        );
        assert!(
            !line.contains("runtime-tokio"),
            "`check-monoio` must not select runtime-tokio:\n  {line}"
        );
    }
}

#[test]
fn the_monoio_job_uses_the_ci_profile_so_known_flakes_retry() {
    // The suite has a load-sensitive flake class (fixed-port listeners, kill-9
    // timing under full-suite parallel load). `.config/nextest.toml`'s `ci`
    // profile carries `retries = 2` for exactly that. A bare `cargo test` has
    // no retries, so it would redden the job intermittently — and an
    // intermittently-red required job gets disabled, which is worse than no
    // job because it still looks like coverage.
    let yaml = workflow();
    let job = job_block(&yaml, "check-monoio").expect("no `check-monoio` job");

    assert!(
        job.contains("cargo nextest run --profile ci"),
        "`check-monoio` must run `cargo nextest run --profile ci`, never a bare \
         `cargo test` — the profile is what supplies retries for the known flake class.\n{job}"
    );
}

#[test]
fn the_monoio_job_cannot_pass_without_running() {
    // Three ways a job can look green while proving nothing.
    let yaml = workflow();
    let job = job_block(&yaml, "check-monoio").expect("no `check-monoio` job");

    assert!(
        !job.contains("continue-on-error"),
        "`check-monoio` must not set continue-on-error — a monoio-only failure has to \
         be able to block a merge, or the job is advisory and will be ignored.\n{job}"
    );
    assert!(
        !job.contains("MOON_NO_URING"),
        "`check-monoio` must NOT set MOON_NO_URING. The tokio jobs set it; this job exists \
         to exercise the io_uring driver that actually ships on Linux.\n{job}"
    );
    assert!(
        job.contains("self-hosted"),
        "`check-monoio` must run on the self-hosted Linux runner — it is the only runner \
         where monoio's io_uring driver executes at all.\n{job}"
    );
}

#[test]
fn the_monoio_job_does_not_share_artifacts_with_the_tokio_job() {
    // `check` and `check-monoio` build INCOMPATIBLE feature sets from one
    // checkout and can run concurrently on the same self-hosted runner. Sharing
    // a target dir would make them invalidate each other's cache continuously,
    // and could let one job execute the other's binaries.
    let yaml = workflow();
    let mono = job_block(&yaml, "check-monoio").expect("no `check-monoio` job");
    let tokio = job_block(&yaml, "check").expect("no `check` job");

    let dir_of = |j: &str| -> Option<String> {
        j.lines()
            .find(|l| l.trim_start().starts_with("CARGO_TARGET_DIR:"))
            .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
    };

    let m = dir_of(&mono).expect("`check-monoio` must set its own CARGO_TARGET_DIR");
    let t = dir_of(&tokio);
    if let Some(t) = t {
        assert_ne!(
            m, t,
            "`check-monoio` and `check` must not share CARGO_TARGET_DIR ({m}) — they build \
             incompatible feature sets and may run concurrently on the same runner."
        );
    }
}

#[test]
fn the_tokio_job_is_still_covered() {
    // This task ADDS a runtime to CI; it must not trade one blind spot for
    // another. tokio remains a supported runtime (portability, Windows).
    let yaml = workflow();
    let tokio = job_block(&yaml, "check").expect("no `check` job");
    assert!(
        tokio.contains("runtime-tokio"),
        "the tokio `check` job must keep testing runtime-tokio — adding monoio coverage \
         must not remove tokio coverage.\n{tokio}"
    );
}
