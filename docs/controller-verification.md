# Phase 3 verification

Verified on 2026-09-29 with OpenJDK 21.0.12.1 (Ubuntu), Maven 3.8.7, Ubuntu 24.04 under WSL2 kernel 5.15.153.1. Controller roots and the final build workspace used the Linux ext4 filesystem (`/dev/sdc`); directory `FileChannel.force` probing succeeded. Native Windows unit runs use simulated directory persistence and do not establish controller durability support.

`mvn clean verify dependency:copy-dependencies` passed **262 tests, zero failures, errors or skips**, covering Phases 1, 2 and 3. The first full Linux run exposed a test cleanup assumption that a second close must finish within 100 ms. Cleanup now retries within a bounded five seconds; the blocked-disk timeout and retained-root-lock assertions remain. The corrected targeted test and full suite passed.

The final independent review found five important issues. Each was reproduced by a failing regression before its fix, followed by the final full suite: delayed append across leadership changes no longer retains stale topic reservations; paused disk pressure and large replica responses remain bounded and recover without treating queue saturation as storage failure; outbound DTO/encode memory is charged before queue admission; idle sockets cannot block peer establishment/reconnect and have a deadline even before their first byte; supported minimum-size batches admit the longest topic name and split coalesced creates with their barrier ordered last. Snapshot admission rejection also cancels the download so a later Fetch can retry. Codec tests check size preflight against actual bytes for all operations. See the tightened [configuration limits](controller-configuration.md).

The three strict process tests passed real TCP create/read, kill of a leader after an acknowledged create, stable UUID after failover, restart of all controllers, minority barrier rejection, and offline follower catch-up. Catch-up requires an actual nonzero segment prefix after deletion, a changed follower generation, positive snapshot end, and equality of all 24 topic UUIDs. The test launcher uses production recovery/runtime behind exact-frame proxies; only test bind addresses and small segment/snapshot thresholds differ from defaults. Child logs are retained under `target/controller-process-logs`.

A direct CLI smoke also passed using three production `ControllerMain` processes and fresh ext4 roots: generate UUID; format all three roots; start all nodes; create `orders`; linearizable metadata; local metadata from node 1; describe quorum; graceful shutdown. Create and both catalogs returned the same topic UUID, commit/applied 3, epoch 1, with a ready leader and follower durable matches at 3. See [runnable commands](controller-configuration.md).

Deterministic campaigns passed 100 seeds, each with 2000 fault steps and a healthy suffix. Default checked-in seeds are 1, 7, 42 and 20260928; additional seeds use `index * 7919`. An independent oracle preserves committed content, persisted vote choices and acknowledged UUIDs across crashes and snapshots. The bounded history checker enumerates serial orders for at most eight admin invocations, independent of internal commit offsets. Deliberately broken test fixtures prove detection of acknowledgement before durability, forgotten votes and stale barrier reads. Unknown operations remain pending and may take effect after their timeout response.

Reproduce from a Linux/ext4 checkout with Java 21:

```bash
mvn clean verify dependency:copy-dependencies
mvn -Dtest=QuorumFaultCampaignTest,LinearizabilityHistoryTest -Dquorum.seedCount=100 test
mvn -Dtest=ControllerClusterTest,ControllerCrashTest,ControllerSnapshotCatchupTest test
```

Process kills test process crash with the OS page cache retained. Simulated power loss in `FaultFiles` and `FakeDisk` tests models forced versus unforced state; it is distinct from a physical host power failure. The tests do not certify every filesystem or storage device.

Scope remains three fixed voters and metadata descriptors only. Broker integration, partition assignment/replication, dynamic membership, TLS/authentication, rolling upgrades and recovery of a permanently lost voter are later phases. Journal compaction is not implemented: reaching its 64 MiB cap fails explicitly. Epoch indexes rebuild after append; optimization is future work. Client attempts use fresh channels and can incur connection churn.

Deferred minor: bounded role-transition/failure logging and DescribeQuorum pending/queue/budget counters are incomplete. Current status remains observable, but overload and election diagnosis needs further instrumentation.
