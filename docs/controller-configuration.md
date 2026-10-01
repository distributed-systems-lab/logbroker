# Metadata quorum configuration and example

Java 21, Maven 3.9, Linux/WSL on a filesystem supporting directory force are required for strict controllers. Use WSL ext4 (for example a project under `/tmp` or your Linux home), then follow the commands below in Bash. See [operation and failure contract](controller-operation.md). Keep controller roots separate from Phase 2 broker directories. Do not reuse existing user data.

Properties are strict: unknown keys fail startup. Relative `data.dir` is relative to the process working directory. All three voters must use identical cluster UUID and voter list, different node IDs, and unique roots. The sample UUID is an example only; replace it in all three files with the newly generated UUID **before formatting or starting**.

| Property | Default | Limit / relation |
|---|---:|---|
| `cluster.id`, `node.id`, `voters`, `data.dir` | required | three unique `id@host:port` voters; local ID in set |
| `fetch.idle.wait.ms` | 100 | positive, below RPC timeout |
| `rpc.timeout.ms` | 1000 | positive, below election minimum |
| `election.min.ms`, `election.max.ms` | 1500, 3000 | min < max |
| `leader.contact.timeout.ms` | 3000 | positive |
| `admin.timeout.ms`, `shutdown.timeout.ms` | 30000 | positive; wire admin timeout <= 30000 |
| `max.frame.bytes` | 8388608 | 1024..8388608, excludes length prefix |
| `fetch.max.bytes` | 4194304 | at least max batch, at most frame minus 1024 |
| `snapshot.chunk.bytes` | 262144 | 1..262144, at most frame minus 1024 |
| `snapshot.max.bytes` | 67108864 | catalog envelope fits, <=67108864; v1 image also structurally bounded to 36480 bytes |
| `snapshot.trigger.bytes` | 16777216 | positive; appended storage batch bytes since last snapshot |
| `max.pending.requests` | 1024 | 1..1024 |
| `event.queue.capacity` | 4096 | >512 + disk capacity, <=4096 |
| `disk.queue.capacity` | 256 | 32..256; completion slots reserved before disk admission; 16 reserved against ordinary append/snapshot pressure |
| `inbound.bytes`, `outbound.bytes` | 67108864 | at least max frame, <=67108864; outbound also >=32 times max batch; one eighth reserved for peer control |
| `max.topics`, `max.partitions` | 128, 1024 | topics 1..128; total partitions topics..1024 |
| `log.segment.bytes`, `log.max.batch.bytes`, `log.index.interval.bytes` | 67108864, 1048576, 4096 | segment >= batch >=336; positive index interval; batch <= fetch budget |

Metadata log defaults: 64 MiB segments, 1 MiB batches, 4096-byte sparse index interval. Journal frames <=64 KiB and journal file <=64 MiB; reaching the cap fails explicitly (journal compaction is future work). Transport caps: 64 connections, 32 pending requests per connection, 256 validation jobs with 64 reserved for control, 30-second partial-frame deadline, two pinned snapshot uploads with 30-second idle expiry. Snapshots keep latest two plus active-generation references and active pins. Client caps: 1024 active invocations, one RPC per attempt connection, shared 64 MiB inbound/outbound budgets, 30-second absolute invocation deadline, 1-second attempt deadline, 50 ms exponential retry capped at 1 second with deterministic jitter.

Admin admission is additionally bounded by `min(max.pending.requests, (disk.queue.capacity - 16) / 4)`, or 60 with defaults. Proposal cohorts split into batches while their read barrier remains after all captured creates. A follower appends fetched batches sequentially. Queue rejection returns overload and permits retry; actual storage failure still fails the controller.

Connections must identify within the RPC timeout, even if they send no bytes. At the global connection cap, an unclassified/admin connection may be evicted to admit another connection; up to eight classified peer connections are protected. A disconnected admin invocation can have an unknown outcome and must use the documented retry semantics.

Outbound DTOs reserve budget before entering the validation queue. The conservative charge includes eight times encoded size plus per-element overhead, held through socket write. Log reads reserve memory before loading and return at most one configured max-batch byte budget per response. These limits favor bounded memory and control progress; they may reduce throughput or require additional Fetch RPCs under pressure.

Build and format in a fresh Linux directory:

```bash
mvn verify dependency:copy-dependencies
CP='target/classes:target/dependency/*'
CLI=vn.huyqt.logbroker.controller.client.ControllerCli
MAIN=vn.huyqt.logbroker.controller.ControllerMain
cluster=$(java -cp "$CP" "$CLI" generate-cluster-id)
voters='0@127.0.0.1:19090,1@127.0.0.1:19091,2@127.0.0.1:19092'
sed -i "s/^cluster.id=.*/cluster.id=$cluster/" config/controller-{0,1,2}.properties
java -cp "$CP" "$CLI" format --data target/controller-0 --node 0 --cluster "$cluster" --voters "$voters"
java -cp "$CP" "$CLI" format --data target/controller-1 --node 1 --cluster "$cluster" --voters "$voters"
java -cp "$CP" "$CLI" format --data target/controller-2 --node 2 --cluster "$cluster" --voters "$voters"
```

Start each node in its own terminal from the same project directory:

```bash
java -cp 'target/classes:target/dependency/*' vn.huyqt.logbroker.controller.ControllerMain --config config/controller-0.properties
java -cp 'target/classes:target/dependency/*' vn.huyqt.logbroker.controller.ControllerMain --config config/controller-1.properties
java -cp 'target/classes:target/dependency/*' vn.huyqt.logbroker.controller.ControllerMain --config config/controller-2.properties
```

Admin terminal (reuse `cluster`, `voters`, `CP`, `CLI`):

```bash
java -cp "$CP" "$CLI" create-topic --cluster "$cluster" --voters "$voters" --name orders --partitions 3
java -cp "$CP" "$CLI" metadata --cluster "$cluster" --voters "$voters"
java -cp "$CP" "$CLI" local-metadata --cluster "$cluster" --voters "$voters" --node 1
java -cp "$CP" "$CLI" describe-quorum --cluster "$cluster" --voters "$voters" --node 0
```

Create prints `CREATED topicId=<UUID>`; retry with the same name/count returns the same UUID. Metadata prints `consistency=LINEARIZABLE` only after a fresh committed barrier; local read prints `consistency=LOCAL node=1` and may lag. Both expose epoch, leader, exclusive commit and applied offsets, followed by topic descriptors. Describe adds role, exclusive log/durable/snapshot ends, readiness and leader-observed durable matches. Timeout after possible transmission is `UNKNOWN`; read metadata or retry the same create name/count to resolve it.

Ctrl+C drains each controller before releasing its lock. Restart with the same config without reformatting. Phase 3 creates metadata descriptors only; Phase 2 broker roots do not receive these topics yet.
