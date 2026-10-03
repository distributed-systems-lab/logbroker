# Phase 4 execution progress

Branch: `feat/phase-4-cluster`. Execution uses the existing checkout, inline, with no
Git worktree, as requested on 2026-10-03. Baseline: `1f7f498`.

This is an implementation progress record, not Phase 4 acceptance. The remaining
tasks in [the implementation plan](2026-10-03-cluster-phase-4.md) still need execution.

| Task | Status | Commit |
|---|---|---|
| 1 Versioned metadata records/limits/entry codec | Complete | `f782c56` |
| 2 Atomic image apply and snapshot schema v2 | Complete | `8016a2b` |
| 3 Broker control RPC models/codecs and role isolation | Complete | `3952e01` |
| 4 Feature bootstrap, registration, atomic assignment | Complete | `70ae5a3` |
| 5 Heartbeat liveness and committed fencing | Complete | `060dcd6` |
| 6 Committed observer endpoints and budgets | Complete | `9fd270b` |
| 7 Broker identity/format/config | Complete; initialization helpers extracted in Tasks 8/11 | See Task 7 commit |
| 8 Durable observer generations/snapshot journal | Complete | `7fe770d` |
| 9 Broker control client and metadata pull | Complete | `c03b772` |
| 10 Lifecycle and serving gate | Complete | `8ffddca` |
| 11 Partition inventory/provisioning | Complete | `d420a4d` |
| 12 Cluster broker composition/forwarding | Complete | `7345b17` |
| 13 Data wire v2 and guarded I/O | Complete | `2f049ce` |
| 14 Cluster client/routing/retries | Complete | See Task 14 commit |
| 15 Cluster process/fault acceptance | Pending | |
| 16 Operations/examples/documentation/final verification | Pending | |

## Verification evidence

Task 14, 2026-10-03:

- ClusterClient pins cluster identity, guards metadata offsets/topic UUID, replaces endpoint
  connections and bounds operations, queued bytes and live connections. Produce routes by owner,
  retries only safe rejected entries and preserves partial successful/UNKNOWN results in order.
- Producer/Consumer now use RequestClient; original deadlines survive discovery/backoff/retry.
  Fetch polls share one global budget/minimum, delegate the oversized exception once and retain offsets.
- New regression reproduced application callbacks running under the routing lock. Futures now publish
  after releasing that lock; metadata, Produce and close callback regressions pass.
- Other regressions cover silent bootstrap failover, foreign cluster, single-flight refresh,
  metadata regression/endpoint replacement, same-name different UUID, cancellation/bounds,
  sent CreateTopic deadline UNKNOWN, partial UNKNOWN and ordered Producer retries.
- Windows `mvn clean verify`: 416 tests, zero failures/errors, three strict process skips,
  completed 23:04:52. Fresh WSL/ext4 `mvn clean verify`: 416 tests, zero failures/errors/skips,
  completed 23:06:58. Evidence: `target/phase4-task14-wsl/` (ignored build output).
- Real three-controller/three-broker data and fault acceptance remains Task 15. Task 16
  still owns production examples/operator documentation and final acceptance.

Task 13, 2026-10-03:

- Independent bootstrap vector, v2 route/outcome/boundary checks and preserved v1 vectors pass.
  Admission checks cluster, session and leader epochs before scheduling and before partition I/O.
- Focused Checkpoint B broker/protocol/lifecycle suite: 43 tests, zero failures/errors/skips,
  completed 22:34:12 before final additional regressions.
- Final Windows `mvn clean verify`: 400 tests, zero failures/errors, three strict process skips,
  completed 22:40:31. Fresh WSL/ext4 `mvn clean verify`: 400 tests, zero failures/errors/skips,
  completed 22:41:35. Logs/reports: `target/phase4-task13-wsl/` (ignored output).
- New regressions cover queued fencing, already-appended FLUSHED fencing, blocked append timeout,
  delayed mutation completion, partial multi-partition success, durable-prefix Fetch, delegated
  oversized batch, real TCP bootstrap/v1 rejection and malformed outcome/count bounds.
- A deterministic test found fencing lost behind the lane's coalescing flush control slot.
  Permission changes now set a pending flag consumed by the common tick; the regression passes.
  Flush advancement also notifies Fetch waiters; long polling wakes without waiting for a deadline.
- Native Windows remains core verification, not strict durability evidence. Tasks 14–16 remain.

Task 12, 2026-10-03:

- Production Broker/BrokerMain now require explicit cluster config and a formatted root.
  Local metadata authority and standalone startup exist only in historical test fixtures.
- Focused forwarding/composition/legacy-regression/CLI suite: 17 tests, zero failures/errors/skips.
- Final Windows `mvn clean verify`: 384 tests, zero failures/errors, three strict process skips,
  completed 22:16:02. Fresh WSL/ext4 `mvn clean verify`: 384 tests, zero failures/errors/skips,
  completed 22:19:32. Logs/reports in `target/phase4-task12-wsl/` (ignored output).
- An initial full run reproduced TCP disconnect overtaking a received controller reply's decode.
  A blocked-decode regression confirmed the race; transport now drains received frames first.
  Focused drain/control/CLI tests passed (10 tests); final full suite also passed.
  The selector included a nonexistent AdminClientRetryTest; actual ControllerClientRetryTest
  subsequently ran in the full suite. No assertions or timeouts were weakened.
- Startup bind failure releases storage/root ownership; shutdown drains before root release.
  Observer lifecycle callbacks marshal to the scheduler to avoid lock inversion.
- This is composition verification. Real data v2/client/process acceptance remains Tasks 13–16.

Task 11, 2026-10-03:

- Focused provisioning/inventory/manager/lane/storage suite: 16 tests, zero failures/errors/skips.
- Windows full `mvn clean verify`: 378 tests, zero failures/errors, three strict process skips,
  completed 20:50:32.
- Fresh WSL Ubuntu source copy on verified ext4: 378 tests, zero failures/errors/skips,
  completed 20:50:54. Logs/reports: `target/phase4-task11-wsl/` (ignored output).
- Provisioning faults cover every measured force boundary. Missing COMPLETE logs stay unavailable;
  obsolete open completion cannot publish. A reserved partition-lane close barrier drains I/O first.
- The previous attempt to stage/commit Task 11 was not executed because automatic approval review
  hit its usage limit. On continuation, staging/check/commit succeeded without bypassing review.

Task 8 and initial Task 9, 2026-10-03:

- Task 8 Windows full suite before Task 9: 345 tests, zero failures/errors, three strict process skips.
- Observer persistence/install/shared snapshot regressions plus initial control client/observer:
  34 tests, zero failures/errors/skips, completed 20:11:10 on Windows.
- Fresh WSL Ubuntu source copy on verified ext4: `mvn clean verify`, 355 tests,
  zero failures/errors/skips, completed 20:12:46 Asia/Saigon. This includes initial Task 9;
  later Task 9 changes still require verification. Evidence was copied to
  `target/phase4-wsl-evidence/` (ignored build output).
- An earlier WSL full run had one ControllerCrashTest convergence timeout after restart
  (349 tests, one failure). A focused rerun passed, followed by the full 355-test pass.
  The transient timeout's root cause is not established; no test assertions/timeouts were weakened.
- SnapshotJournal borrows the existing journal without introducing voter hard state in the
  observer. Forced observer checkpoints cover only received contiguous batches. Cancellation
  before generation publication leaves the old log usable; published corruption fails recovery.
  Cleanup preserves active generations and snapshot pins.
- Final Task 9 focused broker control/observer/store/admin-retry/wire limits suite:
  21 tests, zero failures/errors/skips, completed 20:15:04 on Windows. Includes a real
  Netty broker discovery connection, bounded RPC queue and duplicate callback admission,
  remaining wire timeout, disk-draining stop and corrupt snapshot retry.
- Task 10 lifecycle/gate/observer/control focused suite: 20 tests, zero failures/errors/skips,
  completed 20:26:29 on Windows. Covers fresh registration, original incarnation/CAS on retry,
  applied recovery grant before RUNNING, controller absence for 60 seconds without lease expiry,
  committed fencing, fixed recovery target, old grant refusal and storage identity mismatch.
  The final observer retry change avoids issuing a metadata read barrier after transient fetch errors;
  the same 20-test suite passed again at 20:27:42 before the Task 10 commit.

Checkpoint A (Tasks 4–6), 2026-10-03:

- Task 4 focused admission/identity/config/consensus suite: 33 tests, zero failures/errors/skips.
- Task 4 full Windows `mvn clean verify`: 307 tests, zero failures/errors, three process skips.
- Task 4 WSL/ext4 `mvn -B clean verify`: 307 tests, zero failures/errors/skips,
  completed 18:59:30 Asia/Saigon.
- Task 5 final heartbeat/fencing/manager/config/election/replication/read suite:
  42 tests, zero failures/errors/skips. The repository has no `OrderedDiskExecutorTest`;
  it was accidentally included in one comma-separated selector and contributed no tests.
- Task 6 focused observer/admission/budget/snapshot/replication suite before the final
  retention race case: 23 tests, zero failures/errors/skips.
- Checkpoint A Windows `mvn clean verify`: 329 tests, zero failures/errors, three process skips,
  completed 19:21:24. A final maintenance scheduling change was then verified on WSL.
- Checkpoint A WSL Ubuntu, OpenJDK 21.0.12.1, Maven 3.9.12, ext4 verified by `findmnt`:
  fresh source copy `mvn -B clean verify`, **329 tests, zero failures/errors/skips**,
  completed 19:23:26. Log: `target/phase4-wsl-checkpoint-a.log` (ignored build output).
- Task 7 focused identity/config/format/data-config suite: 12 tests, zero failures/errors/skips,
  completed 19:33:54 on Windows. Expected missing-type/missing-format RED runs preceded implementation.

New regressions observed: a leader with durable but unapplied FeatureLevel could
bootstrap twice; replacement registration could race pending unfence; an unfence
could miss assignments from an unapplied preceding topic; retention could race an
observer read. Each now has regression coverage. Recovery waits for the inherited
prefix, same-broker commands serialize, lifecycle drains wait for prior image apply,
and a retained-away observer prefix asks the caller to fetch a snapshot again.

Native Windows 11, Oracle JDK 21.0.1, Maven 3.9.12:

- Baseline `mvn clean verify`: 262 tests, zero failures/errors, three process skips.
- Task 1 focused codec suite: 16 tests, zero failures/errors/skips.
- Task 2 focused apply/image/snapshot/config/transfer suite: 36 tests, zero failures/errors/skips.
- After Task 2 `mvn clean verify`: 283 tests, zero failures/errors, three process skips.
- Task 3 control/role/codec/transport focused suite: 25 tests, zero failures/errors/skips.
- Task 3 full `mvn clean verify` before the final wrong-hash test: 293 tests, zero
  failures/errors, three process skips. Clean rebuild of the final four role tests passed.

WSL Ubuntu, OpenJDK 21.0.12.1, Maven 3.9.12, verified `ext4` source/build root:

- Fresh source copy of `3952e01`, outside Git, under an ext4 `/tmp` build directory.
- `mvn -B clean verify`: **294 tests, zero failures/errors/skips**, completed
  2026-10-03 18:29:45 Asia/Saigon. All three existing strict controller process tests ran.
- The full log is retained as `target/phase4-wsl-verify.log` (ignored build output).
  This verifies the current checkpoint; it does not establish the future Phase 4
  three-controller/three-broker acceptance or the 100-seed campaign.

Expected RED failures were observed before the new schema, atomic apply, v2 snapshot
and broker role decoder changes. A large-image regression found residual v1 bounds
in file load/download/transfer; those now use the configured envelope cap while
preserving independent chunk bounds.

An incremental Windows run was affected by the VS Code Java language server writing
problem classes into Maven's test output. `clean` rebuilt with javac successfully;
the final WSL copy used separate output and passed the whole suite. No IDE process
was stopped or user settings changed.

## Implementation details for continuation

- V1 constructors/vectors remain available for historical regressions. V2 `MetadataImage`
  starts with feature level zero and accepts cluster mutations only after FeatureLevel(2).
- Task 2 additionally touched `GenerationStore` and `SnapshotTransfer`: the former needs
  an explicit schema/limits replay overload; the latter retained a v1 snapshot cap.
- Task 3 additionally preserves the request's envelope version in consensus peer replies.
  `ControllerService.request(Request,long)` was already public and bounded, so no duplicate
  service API was introduced.
- Role whitelist applies to requests. Only controllers (VOTER role) emit responses,
  including 110–113; this prevents the request whitelist from rejecting broker replies.
- Observer batches cannot exceed their advertised committed boundary. Role, sender ID,
  schema and broker incarnation are pinned to a TCP connection; broker bulk traffic uses
  shared budgets. Existing voter reserve tests and v1 protocol tests remain green.
- V2 formatting is opt-in through `--metadata-version 2` and startup `metadata.version=2`;
  the production default switch is still Task 16. V2 roots execute registration,
  assignment, heartbeat recovery and committed observer reads. V1 roots remain fixtures
  and explicitly reject full v2 metadata reads. Standalone broker replacement is pending.
- Ordinary heartbeat uses zero recovery UUID; a nonzero UUID captures a fixed target
  and coalesces the unfence/result. A ready leader starts a fresh liveness observation
  window; no broker contact can extend voter check-quorum.
- Observer disk refusal returns OVERLOADED without stepping down the leader. Shared
  bulk memory and ordinary completion admission preserve voter reserves. Uploads share
  a two-pin cap across role namespaces and expire/close on the ordered worker.
- Task 7 format publishes identity last after a forced observer generation/log/journal and inventory
  INIT frame. The initialization helpers are extracted into full readers in Tasks 8/11 before
  production composition. Cluster roots/configs are refused by the transitional standalone launcher,
  so a formatted root cannot silently run with standalone semantics. Legacy launch fixtures remain
  until the Task 12/16 composition switch. Format tests use FaultFiles, not Windows durability claims.
- Next: Task 15 real cluster process/fault acceptance.
