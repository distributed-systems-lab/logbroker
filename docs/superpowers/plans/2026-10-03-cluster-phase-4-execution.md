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
| 4 Feature bootstrap, registration, atomic assignment | Pending | |
| 5 Heartbeat liveness and committed fencing | Pending | |
| 6 Committed observer endpoints and budgets | Pending | |
| 7 Broker identity/format/config | Pending | |
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
- Controller roots/runtime still use the legacy composition until Task 4 wires feature
  bootstrap and the cluster control manager. V2 discovery is available, but legacy roots
  explicitly reject full v2 metadata reads. Broker control DTOs do not themselves execute
  registration, heartbeat or observer reads. Standalone broker replacement is still pending.
- Next: follow Task 4's RED tests, integrate atomic command draining into the existing
  consensus append/commit/apply path, and wire schema/limits consistently into node recovery.
