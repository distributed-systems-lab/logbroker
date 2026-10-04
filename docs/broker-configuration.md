# Broker configuration

This is the historical Phase 2 contract. Production standalone startup has been
replaced by the [cluster broker composition](cluster-broker-composition.md).
The data-plane limit properties below still apply to cluster brokers.

Production requires both `--config` and `--data`, an explicitly formatted identity,
and reachable controller bootstrap endpoints. Follow [cluster configuration](cluster-configuration.md)
for format/start commands and [operations](cluster-operation.md) for lifecycle/status semantics.

`BrokerMain` requires `--data <directory>`. It binds `127.0.0.1:9092` by default. `--host` and `--port` override the corresponding properties from `--config <properties-file>`. Unknown options, unknown properties, invalid numeric values, and incompatible limits fail startup. A broker holds an exclusive `.broker.lock` in its data directory until storage has closed.

| Property | Default | Unit |
| --- | ---: | --- |
| `host` | `127.0.0.1` | bind address |
| `port` | `9092` | TCP port (`0` for ephemeral) |
| `flushIntervalMs` | `10` | ms |
| `flushBytes` | `1048576` | storage bytes |
| `maxFetchBytes` | `4194304` | wire batch bytes |
| `maxFrameBytes` | `8388608` | bytes |
| `maxWireBatchBytes` | `1048576` | bytes |
| `maxStorageBatchBytes` | `1048576` | bytes |
| `maxPartitionEntries` | `64` | entries/request |
| `maxRecordsPerBatch` | `10000` | records/batch |
| `segmentBytes` | `67108864` | bytes |
| `maxBatchBytes` | `1048576` | bytes |
| `indexIntervalBytes` | `4096` | bytes |
| `dataWorkers` | `4` | threads |
| `validationWorkers` | `2` | threads |
| `maxConnections` | `128` | connections |
| `maxTasksPerPartition` | `256` | queued tasks |
| `maxValidationTasks` | `256` | queued tasks |
| `maxQueuedRequestBytes` | `67108864` | bytes |
| `maxOutboundPerConnection` | `16777216` | bytes |
| `maxOutboundTotal` | `67108864` | bytes |
| `maxRequestContexts` | `1024` | requests |
| `maxFlushedWaiters` | `4096` | waiters |
| `maxTopics` | `128` | topics |
| `maxPartitions` | `1024` | total partitions |
| `shutdownTimeoutMs` | `30000` | ms |

All capacities and timeouts must be positive, except `port=0`. Frame, wire batch, storage batch, Fetch, and outbound limits must pass the cross-validation in `BrokerConfig`, `ProtocolLimits`, and `LogConfig`. The CLI property file uses Java `.properties` syntax.

`APPENDED` confirms assignment to the local log after append; `FLUSHED` confirms local force to disk. Fetch may see records before flush. A lost Produce response has an unknown outcome; the Java client never retries it automatically because replay may create a duplicate. Metadata has a separate local log and strict replay; corruption stops startup. A failed data partition is isolated and reported unavailable. Phase 2 is a single-broker learning system; it has no replication, consensus, consumer groups, security, or Kafka wire compatibility.
