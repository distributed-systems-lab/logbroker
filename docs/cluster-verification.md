# Phase 4 verification

Strict acceptance requires Java 21, Maven 3.9.x, Linux/WSL, an ext4 checkout/data roots and successful DurableFiles directory-force probes. `/mnt/d` and native Windows do not establish durability. Report platform-guarded tests as skips.

```bash
export MAVEN_OPTS='-Xms32m -Xmx512m'
mvn '-DargLine=-Xms32m -Xmx512m' clean verify dependency:copy-dependencies
mvn '-DargLine=-Xms32m -Xmx512m' -Dtest=ClusterFaultCampaignTest -Dcluster.seedCount=100 test
mvn '-DargLine=-Xms32m -Xmx512m' -Dtest=QuorumFaultCampaignTest,LinearizabilityHistoryTest -Dquorum.seedCount=100 test
mvn '-DargLine=-Xms32m -Xmx512m' -Dtest=ClusterRoutingTest,ClusterControllerLossTest,ClusterBrokerRestartTest,ClusterObserverCatchupTest,ClusterProvisioningCrashTest,BrokerCliSmokeTest test
bash scripts/cluster-demo.sh /tmp/logbroker-acceptance-new
```

Inspect Surefire XML for every selected class, zero failures/errors and zero strict-test skips. Retain `target/controller-process-logs` and `target/cluster-process-logs`. Exit zero cannot prove missing/skipped tests ran.

The deterministic cluster campaign uses checked-in seeds in `src/test/resources/cluster/fault-seeds.txt` and reproducible extra seeds for 100-seed runs. It drives controller state machines, broker lifecycle/control codecs, observer journal and routing client, comparing applied state with an independent committed-record model. It checks bounded queues, serving authority and no automatic retry after a lost Produce reply. Four deliberately broken oracle cases detect duplicate retry, uncommitted apply, stale unfence and partial publication inside a committed topic batch. This is separate from real process and persistence-force testing.

Six-process tests cover six partitions on three owners from one bootstrap; isolated controller links and lost voter majority; fresh recovery after restart with retained FLUSHED records; snapshot catch-up after actual prefix deletion; and a missing COMPLETE log remaining unavailable. Observer install/provisioning tests separately cover force and power-loss boundaries.

Exact evidence and pending gates are tracked in [the execution record](superpowers/plans/2026-10-03-cluster-phase-4-execution.md). Final acceptance remains pending until the final full suite, campaigns, CLI demo and review pass.

## Recorded results: 2026-10-04

Platform: WSL Ubuntu, ext4 (`findmnt`), OpenJDK 21.0.12.1, Maven 3.9.12.
Formatted process roots passed DurableFiles directory-force probing. Source baseline HEAD
`5608e5a` plus the Tasks 15–16 working changes; final commit identifiers are in the execution record.
Each test JVM used `-Xms32m -Xmx512m`; owned child nodes used `-Xms32m -Xmx256m`.

| Command group | Tests | Failures/errors/skips | Finished (Asia/Saigon) |
|---|---:|---|---|
| clean verify dependency:copy-dependencies | 430 in 132 classes | 0 / 0 / 0 | 00:25:11 |
| ClusterFaultCampaignTest, cluster.seedCount=100 | 3 | 0 / 0 / 0 | 00:26:13 |
| QuorumFaultCampaignTest + LinearizabilityHistoryTest, quorum.seedCount=100 | 7 | 0 / 0 / 0 | 00:26:27 |
| Six selected process/CLI classes | 9 | 0 / 0 / 0 | 00:29:27 |

Surefire XML was inspected for the exact selected classes, rather than counting stale reports
from preceding invocations. Cluster seeds were `1, 7, 42, 20261003` plus `index * 7919` for
indices 4–99; each seed drove 2000 actions followed by a healthy recovery suffix.

The actual six-node demo exited zero with `SUCCESS records=6 brokers=3`, three broker sessions,
six assignments, linearizable metadata and LOCAL metadata. Cleanup completed and retained
logs/data. Evidence is under ignored `target/phase4-final-wsl/`.

The last BrokerMain resource-close refinement was rebuilt and exercised by the subsequent
campaign/process/demo phases. The subsequent fresh full verification stalled with WSL and has
no completion evidence. On 2026-10-04 the user deferred further WSL testing. Implementation
can be committed with the recorded evidence, but final strict acceptance remains deferred;
native Windows verification cannot replace the ext4 durability/process gate.

Fresh native Windows verification of the final Java source completed 2026-10-04
10:27:50 Asia/Saigon: Java 21, Maven 3.9.12, `clean verify dependency:copy-dependencies`,
430 tests in 132 classes, zero failures/errors, 11 platform skips, BUILD SUCCESS.
Build output and TEMP/TMP were kept on D; the log is `target/phase4-windows-final.log`.
The skipped process tests are not counted as passed acceptance tests.
