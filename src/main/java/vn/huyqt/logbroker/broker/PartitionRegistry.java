package vn.huyqt.logbroker.broker;

import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.TopicInfo;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Owns open user partition logs and isolates an unusable data partition.
 *
 * <p>Each partition of a topic lives in {@code topics/<topic-id>/<partition-id>/} under the data
 * directory; topic names are never used as paths. A partition that failed to open or failed later
 * is remembered as unavailable until the registry is closed, so it is never reopened while the
 * broker runs. All methods are synchronized.
 */
public final class PartitionRegistry implements AutoCloseable {
    private final BrokerConfig config;
    private final PartitionStore.Factory factory;
    private final Map<TopicPartition, PartitionStore> stores = new HashMap<>();
    private final Map<TopicPartition, IOException> failures = new HashMap<>();

    public PartitionRegistry(BrokerConfig config, PartitionStore.Factory factory) {
        this.config = Objects.requireNonNull(config);
        this.factory = Objects.requireNonNull(factory);
    }

    /**
     * Opens every partition of {@code topic} that is not already open or failed. An open failure is
     * recorded for that partition only and is not thrown.
     */
    public synchronized void initialize(TopicInfo topic) {
        for (var partition : topic.partitions()) {
            var tp = new TopicPartition(topic.id(), partition.partition());
            if (stores.containsKey(tp) || failures.containsKey(tp)) continue;
            Path directory =
                    config.dataDirectory()
                            .resolve("topics")
                            .resolve(topic.id().toString())
                            .resolve(Integer.toString(tp.partition()));
            try {
                stores.put(tp, factory.open(directory, config.logConfig()));
            } catch (IOException error) {
                failures.put(tp, error);
            }
        }
    }

    /**
     * Returns the open store for {@code partition}.
     *
     * @throws IOException if the partition is unknown, or is unavailable (with the recorded failure
     *     as cause)
     */
    public synchronized PartitionStore require(TopicPartition partition) throws IOException {
        PartitionStore store = stores.get(partition);
        if (store != null) return store;
        IOException failure = failures.get(partition);
        if (failure != null) throw new IOException("Partition unavailable: " + partition, failure);
        throw new IOException("Unknown partition: " + partition);
    }

    /**
     * Returns {@link ErrorCode#NONE} for an open partition, {@link ErrorCode#PARTITION_UNAVAILABLE}
     * for a failed one, and {@link ErrorCode#UNKNOWN_PARTITION} otherwise.
     */
    public synchronized ErrorCode state(TopicPartition partition) {
        if (stores.containsKey(partition)) return ErrorCode.NONE;
        return failures.containsKey(partition)
                ? ErrorCode.PARTITION_UNAVAILABLE
                : ErrorCode.UNKNOWN_PARTITION;
    }

    /**
     * Isolates {@code partition} after a runtime I/O failure: closes its store and reports it
     * unavailable from now on. A close failure is added as suppressed to the recorded cause.
     */
    public synchronized void markFailed(TopicPartition partition, Throwable cause) {
        PartitionStore store = stores.remove(partition);
        failures.put(
                partition,
                cause instanceof IOException io ? io : new IOException("Partition failed", cause));
        if (store != null) {
            try {
                store.close();
            } catch (IOException error) {
                failures.get(partition).addSuppressed(error);
            }
        }
    }

    /**
     * Closes every open store, attempting all of them, and forgets recorded failures.
     *
     * @throws IOException the first close failure, with later ones suppressed
     */
    @Override
    public synchronized void close() throws IOException {
        IOException failure = null;
        for (PartitionStore store : stores.values()) {
            try {
                store.close();
            } catch (IOException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
        }
        stores.clear();
        failures.clear();
        if (failure != null) throw failure;
    }
}
