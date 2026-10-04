# Phase 4 operations

Wait for `STATUS ... state=RUNNING`; `READY <port>` only proves listener binding. Startup registers a fresh session, recovers a fixed committed metadata target, reconciles durable inventory and applies an unfence grant. Broker observers never vote or apply uncommitted records.

RUNNING brokers retain their known RF1 serving permission during control-link failure or loss of voter majority. A known committed fence, session change or local storage failure stops serving. Restart requires fresh registration/recovery; cached permission cannot authorize it. No timeout grants serving authority.

`Broker.status()` is an immutable best-effort diagnostic snapshot. BrokerMain samples transitions every 100 ms and prints changes plus a five-second periodic status. Fields include storage identity/session, lifecycle, controller hint, heartbeat age (`-1` before an accepted heartbeat), applied/durable offsets, snapshot base/generation, partition counts and transport/disk budgets. Diagnostics never grant permission. Failed partition keys identify paths under the data root without printing record payloads.

```bash
CP='target/classes:target/dependency/*'
CLUSTER=28b76eca-7521-43cf-baaa-3376f059c05b
VOTERS='0@127.0.0.1:19090,1@127.0.0.1:19091,2@127.0.0.1:19092'
java -cp "$CP" vn.huyqt.logbroker.controller.client.ControllerCli describe-quorum --cluster "$CLUSTER" --voters "$VOTERS" --node 0
java -cp "$CP" vn.huyqt.logbroker.controller.client.ControllerCli create-topic --cluster "$CLUSTER" --voters "$VOTERS" --name orders --partitions 6
java -cp "$CP" vn.huyqt.logbroker.controller.client.ControllerCli metadata --cluster "$CLUSTER" --voters "$VOTERS"
java -cp "$CP" vn.huyqt.logbroker.controller.client.ControllerCli local-metadata --cluster "$CLUSTER" --voters "$VOTERS" --node 0
```

Controller `metadata` is linearizable; `local-metadata` reports local/stale consistency. Broker Metadata exposes its applied offset and can lag a CreateTopic commit. Version 2 includes broker sessions/fenced state and partition owners/epochs. One bootstrap broker suffices to discover remote owners.

RF1 provides no replica failover or data migration. Broker downtime makes its partitions unavailable; lost storage can lose acknowledged data. A COMPLETE inventory entry with a missing log stays unavailable, never silently recreated. Preserve identity, observer journal, inventory and partition files for diagnosis. Reformatting cannot recover lost records.

Provisioning forces INTENT, initial files/directory, then COMPLETE. Observer publication follows forced journal commits or a forced snapshot-generation install. Bad transferred snapshots are rejected; actual persistence failure poisons the observer and shuts down the broker. A controller-deleted prefix requires snapshot catch-up. Journal/snapshot caps are enforced; never manually trim live journals to bypass them.

FLUSHED acknowledges local durability; Fetch exposes only the high watermark. Produce REJECTED proves append did not start. Only safe routing/admission errors and transport NOT_SENT are retried within the original deadline. UNKNOWN may have appended and is never automatically retried. Applications reconcile uncertainty; this phase has no producer deduplication/exactly-once guarantee. Fetch retries preserve offsets and callers own consumer progress.

Stop with Ctrl+C/SIGTERM and allow storage drain before reusing roots. The identity lock remains owned until partition/observer work drains. Do not share roots between processes or reuse one live broker ID with different storage identities.
