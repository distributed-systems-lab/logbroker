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
| 8 Durable observer generations/snapshot journal | Pending | |
| 9 Broker control client and metadata pull | Pending | |
| 10 Lifecycle and serving gate | Pending | |
| 11 Partition inventory/provisioning | Pending | |
| 12 Cluster broker composition/forwarding | Pending | |
| 13 Data wire v2 and guarded I/O | Pending | |
| 14 Cluster client/routing/retries | Pending | |
| 15 Cluster process/fault acceptance | Pending | |
| 16 Operations/examples/documentation/final verification | Pending | |

## Verification evidence

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
- Next: Task 8 observer persistence and reusable SnapshotJournal.
