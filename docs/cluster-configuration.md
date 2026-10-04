# Phase 4 cluster configuration

Use Java 21 and Maven 3.9.x. Production is cluster-only: three controller voters and RF1 broker observers. Broker roots require explicit formatting. Controller metadata/admin wire default to version 2. Existing version 1 roots require `metadata.version=1` and CLI `--metadata-version 1`; there is no in-place migration.

Samples in `config/cluster/` share UUID `28b76eca-7521-43cf-baaa-3376f059c05b`. Controller files 1–3 correspond to voter IDs 0–2 on ports 19090–19092. Broker IDs 1–3 advertise ports 9092–9094. Advertised endpoints must be reachable by clients. Change the UUID consistently for a new cluster.

On Linux/WSL from an ext4 checkout:

```bash
mvn clean verify dependency:copy-dependencies
bash scripts/cluster-demo.sh /tmp/logbroker-demo-new
```

The script formats fresh roots, starts all six production JVMs, verifies the example, stops its children on exit and retains logs/data. It never deletes an existing root. Formatting also probes directory-force support.

Manual formatting of all controllers:

```bash
CP='target/classes:target/dependency/*'
CLUSTER=28b76eca-7521-43cf-baaa-3376f059c05b
VOTERS='0@127.0.0.1:19090,1@127.0.0.1:19091,2@127.0.0.1:19092'
for id in 1 2 3; do
  java -cp "$CP" vn.huyqt.logbroker.controller.client.ControllerCli format --data "target/cluster/controller-$id" --node "$((id-1))" --cluster "$CLUSTER" --voters "$VOTERS" --metadata-version 2
done
```

Run each controller command in its own terminal, then format brokers once and run each broker in its own terminal:

```bash
java -cp "$CP" vn.huyqt.logbroker.controller.ControllerMain --config config/cluster/controller-1.properties
java -cp "$CP" vn.huyqt.logbroker.controller.ControllerMain --config config/cluster/controller-2.properties
java -cp "$CP" vn.huyqt.logbroker.controller.ControllerMain --config config/cluster/controller-3.properties
for id in 1 2 3; do
  java -cp "$CP" vn.huyqt.logbroker.broker.BrokerMain format --config "config/cluster/broker-$id.properties" --data "target/cluster/broker-$id"
done
java -cp "$CP" vn.huyqt.logbroker.broker.BrokerMain --config config/cluster/broker-1.properties --data target/cluster/broker-1
java -cp "$CP" vn.huyqt.logbroker.broker.BrokerMain --config config/cluster/broker-2.properties --data target/cluster/broker-2
java -cp "$CP" vn.huyqt.logbroker.broker.BrokerMain --config config/cluster/broker-3.properties --data target/cluster/broker-3
java -cp "$CP" vn.huyqt.logbroker.example.ClientExample 127.0.0.1:9092 demo
```

PowerShell uses `"target/classes;target/dependency/*"`. Native Windows does not establish strict durability support; acceptance requires Linux/WSL ext4.

Bounds: 32 brokers, 128 topics, 1024 total partitions, 1 MiB metadata batches, 8 MiB frames, 256 KiB snapshot chunks, 64 MiB snapshots. Default heartbeat is 1 s, control RPC 2 s and broker-session timeout 10 s. Observer fetch is at most 4 MiB with 100 ms wait. The session timeout is controller policy, not a broker serving lease. See [broker properties](broker-configuration.md), [controller properties](controller-configuration.md) and [client limits](cluster-client.md).

Observer state and partition inventory journals are capped at 64 MiB. Capacity exhaustion fails
explicitly; journal compaction is future work. Do not truncate journals to continue operating.
