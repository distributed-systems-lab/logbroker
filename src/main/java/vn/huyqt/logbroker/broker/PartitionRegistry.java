package vn.huyqt.logbroker.broker;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.TopicInfo;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

/** Owns open user partition logs and isolates an unusable data partition. */
public final class PartitionRegistry implements AutoCloseable {
    private final BrokerConfig config;
    private final PartitionStore.Factory factory;
    private final Map<TopicPartition, PartitionStore> stores = new HashMap<>();
    private final Map<TopicPartition, IOException> failures = new HashMap<>();

    public PartitionRegistry(BrokerConfig config, PartitionStore.Factory factory) {
        this.config = Objects.requireNonNull(config);
        this.factory = Objects.requireNonNull(factory);
    }

    public synchronized void initialize(TopicInfo topic) {
        for (var partition : topic.partitions()) {
            var tp = new TopicPartition(topic.id(), partition.partition());
            if (stores.containsKey(tp) || failures.containsKey(tp)) continue;
            Path directory = config.dataDirectory().resolve("topics")
                    .resolve(topic.id().toString()).resolve(Integer.toString(tp.partition()));
            try {
                stores.put(tp, factory.open(directory, config.logConfig()));
            } catch (IOException error) {
                failures.put(tp, error);
            }
        }
    }

    public synchronized PartitionStore require(TopicPartition partition) throws IOException {
        PartitionStore store = stores.get(partition);
        if (store != null) return store;
        IOException failure = failures.get(partition);
        if (failure != null) throw new IOException("Partition unavailable: " + partition, failure);
        throw new IOException("Unknown partition: " + partition);
    }

    public synchronized ErrorCode state(TopicPartition partition) {
        if (stores.containsKey(partition)) return ErrorCode.NONE;
        return failures.containsKey(partition) ? ErrorCode.PARTITION_UNAVAILABLE
                : ErrorCode.UNKNOWN_PARTITION;
    }

    public synchronized void markFailed(TopicPartition partition, Throwable cause) {
        PartitionStore store = stores.remove(partition);
        failures.put(partition, cause instanceof IOException io ? io
                : new IOException("Partition failed", cause));
        if (store != null) {
            try { store.close(); } catch (IOException error) { failures.get(partition).addSuppressed(error); }
        }
    }

    @Override public synchronized void close() throws IOException {
        IOException failure = null;
        for (PartitionStore store : stores.values()) {
            try { store.close(); } catch (IOException error) {
                if (failure == null) failure = error; else failure.addSuppressed(error);
            }
        }
        stores.clear();
        failures.clear();
        if (failure != null) throw failure;
    }
}
