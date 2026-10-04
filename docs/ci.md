# GitHub verification

The CI workflows build and test changes; they do not publish packages, release tags
or deploy a broker cluster. GitHub-hosted runners own all child processes and test data.

## Required pull request checks

`.github/workflows/ci.yml` runs on pull requests, pushes to `main`, and manual dispatch.
The two stable check names are `verify-linux` and `verify-windows`. Each runs the
Python report-gate tests, then `scripts/ci.py` with its OS profile:

- Ubuntu 24.04: Java 21, Maven 3.9.x, Python 3.12; full `clean verify
  dependency:copy-dependencies`, including Spotless, with no test failures, errors or skips.
- Windows 2022: the same build/format contract, with only the checked-in strict
  process-test Windows exclusions permitted. Windows success establishes compatibility,
  not storage durability.

Java/Python versions and runner labels are explicit; Maven's installed version must be
3.9.x or preflight fails. Maven dependencies are cached. A new commit cancels the previous
CI run for the same PR/ref, and jobs have a 25-minute timeout. No automatic retry hides
test failures. Every Java `*Test.java` source class must have a nonempty Surefire suite.
New platform exclusions require deliberate changes to the allowlist in `scripts/ci.py`.

After the first successful GitHub runs, repository administrators can require
`verify-linux` and `verify-windows` in the branch ruleset for `main`. Those remote settings
are separate from adding workflow files and have not been changed by this implementation.

## Extended acceptance

`.github/workflows/verification.yml` runs daily at 19:00 UTC (02:00 the next day in
Asia/Saigon) on the default branch, or through **Actions → Extended verification →
Run workflow**. Scheduled runs require the workflow to be on the default branch;
GitHub may delay scheduled jobs. Manual runs can select a branch.

It runs the full Linux suite, then separate 100-seed cluster and quorum/history campaigns,
the six selected cluster process/CLI classes, and the actual six-node demo. Each phase
uses the commands/selectors documented in [cluster verification](cluster-verification.md).
The demo must exit zero and report `SUCCESS records=6 brokers=3`. Acceptance has a
45-minute job timeout and is intended as an additional verification, not a PR-only gate.

Before Linux verification, preflight requires ext4 at the checkout/build root and temporary
data root, and fsyncs both directories. Real controller/broker process fixtures also execute
DurableFiles directory-force probes. An incompatible runner filesystem fails the job;
it is never downgraded into skipped durability tests. Deterministic fault tests model
power loss; a hosted process-kill test does not independently prove physical power-loss
behavior on every deployment filesystem.

## Evidence and local execution

Each phase retains Maven output, a fresh copy of Surefire XML, and available controller/
broker process logs even when Maven fails. Evidence is stored under
`$RUNNER_TEMP/logbroker-ci-evidence`, outside `target` so `mvn clean` cannot delete it.
Old Surefire reports are removed before selected runs so they cannot masquerade as new
results. The gate checks expected class presence, nonzero counts, count consistency,
failures/errors and individual skip decisions. It counts only the selected phase's classes.
Successful counts are also written to the GitHub step summary.

Workflow artifacts retain ordinary CI evidence for seven days and extended evidence/demo
logs for fourteen days. Download them from the workflow run, including failed runs.
Artifact names distinguish OS and run attempt. Maven artifacts and dependency caches are
not a release or a deployment.

```text
python -B -m unittest discover -s scripts/tests -v
python -B scripts/ci.py windows
python -B scripts/ci.py linux
python -B scripts/ci.py extended
```

The script locates the repository from its own path. Local runs need Java 21, Maven 3.9.x
and Python 3.10+ on PATH; strict profiles additionally need Linux/ext4, `findmnt` and Bash.
Use a fresh checkout/build copy and a fresh `RUNNER_TEMP` outside that checkout's `target`.
The script intentionally refuses existing per-phase evidence directories rather than
overwriting a previous run. Test heaps are bounded to 512 MiB; owned child node JVMs use
their existing 256 MiB limits. `extended` performs more work than the default four-seed suite.

Actions are pinned to full commit SHAs, checkout does not persist credentials, and workflow
permissions are `contents: read`. PR jobs do not require repository secrets or a self-hosted
runner. The workflows use `pull_request`, not privileged `pull_request_target` execution.
Update pinned actions deliberately when upgrading their reviewed versions.

## Local validation: 2026-10-04

The actual `scripts/ci.py windows` entry point completed successfully in an isolated
Windows source copy at 11:09:20 Asia/Saigon: `clean verify dependency:copy-dependencies`,
430 tests in 132 expected classes, zero failures/errors, 11 allowed platform skips,
and the Spotless gate passed for all 275 Java files. Evidence is under ignored
`target/ci-local-runner/logbroker-ci-evidence/full`; the outer log is
`target/ci-windows-local.log`. Thirteen Python report-gate regression tests passed.
Both workflows passed actionlint 1.7.12, with optional shellcheck integration disabled;
workflow run steps use the Python entry point instead of embedded shell scripts.

No GitHub-hosted workflow, new Linux/ext4 test, or extended campaign was executed locally.
Hosted CI/acceptance results must be observed after these files reach GitHub; this local
validation does not replace that gate or mark the deferred Phase 4 acceptance complete.
