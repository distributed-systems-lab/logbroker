package vn.huyqt.logbroker.protocol;

import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint;
import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.protocol.Protocol.Error;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Data wire v2 values. Route epochs are authorities from one committed metadata image. */
public final class ClusterProtocol {
    private ClusterProtocol() {}

    public static final UUID UNPINNED = new UUID(0, 0);

    /**
     * Per-entry Produce result. REJECTED means no append occurred; UNKNOWN means the broker may
     * have appended the entry, so automatic replay could create duplicates.
     */
    public enum Outcome {
        SUCCESS,
        REJECTED,
        UNKNOWN
    }

    public record Route(
            TopicPartition partition, int brokerId, long brokerEpoch, long leaderEpoch) {
        public Route {
            validPartition(partition);
            if (brokerId < 0 || brokerEpoch < 0 || leaderEpoch < 0)
                throw invalid("Invalid route epoch/identity");
        }
    }

    public record CreateTopic(
            UUID clusterId, String name, int partitions, short replicationFactor, int timeoutMs)
            implements Protocol.Request {
        public CreateTopic {
            cluster(clusterId);
            Objects.requireNonNull(name);
            timeout(timeoutMs);
            if (partitions <= 0 || replicationFactor != 1)
                throw invalid("Only positive RF1 topics are supported");
        }
    }

    public record Metadata(UUID clusterId, List<String> names) implements Protocol.Request {
        public Metadata {
            Objects.requireNonNull(clusterId);
            names = List.copyOf(names);
        }
    }

    public record ProduceEntry(Route route, Batch batch) {
        public ProduceEntry {
            Objects.requireNonNull(route);
            Objects.requireNonNull(batch);
        }
    }

    public record Produce(UUID clusterId, AckMode ack, int timeoutMs, List<ProduceEntry> entries)
            implements Protocol.Request {
        public Produce {
            cluster(clusterId);
            Objects.requireNonNull(ack);
            timeout(timeoutMs);
            entries = List.copyOf(entries);
            if (entries.isEmpty()) throw invalid("Empty Produce");
        }
    }

    public record FetchEntry(Route route, long offset, int maxBytes) {
        public FetchEntry {
            Objects.requireNonNull(route);
            if (offset < 0 || maxBytes <= 0) throw invalid("Invalid Fetch entry");
        }
    }

    public record Fetch(
            UUID clusterId,
            int maxBytes,
            int minBytes,
            int maxWaitMs,
            List<FetchEntry> entries,
            boolean allowOversizedFirstBatch)
            implements Protocol.Request {
        public Fetch {
            cluster(clusterId);
            entries = List.copyOf(entries);
            if (maxBytes <= 0
                    || maxBytes > 4 * 1024 * 1024
                    || minBytes < 0
                    || minBytes > maxBytes
                    || maxWaitMs < 0
                    || maxWaitMs > 5000
                    || entries.isEmpty()) throw invalid("Invalid Fetch budget");
        }

        public Fetch(
                UUID clusterId,
                int maxBytes,
                int minBytes,
                int maxWaitMs,
                List<FetchEntry> entries) {
            this(clusterId, maxBytes, minBytes, maxWaitMs, entries, false);
        }
    }

    public record BrokerInfo(int id, Endpoint endpoint, long brokerEpoch, boolean fenced) {
        public BrokerInfo {
            Objects.requireNonNull(endpoint);
            if (id < 0 || brokerEpoch < 0) throw invalid("Invalid broker descriptor");
        }
    }

    public record PartitionInfo(
            int partition,
            Error error,
            List<Integer> replicas,
            int leaderId,
            long leaderEpoch,
            long partitionEpoch) {
        public PartitionInfo {
            Objects.requireNonNull(error);
            replicas = List.copyOf(replicas);
            if (partition < 0
                    || leaderId < 0
                    || leaderEpoch < 0
                    || partitionEpoch < 0
                    || replicas.size() != 1
                    || replicas.getFirst() != leaderId) throw invalid("Invalid RF1 assignment");
        }
    }

    public record TopicInfo(String name, UUID id, List<PartitionInfo> partitions) {
        public TopicInfo {
            Objects.requireNonNull(name);
            cluster(id);
            partitions = List.copyOf(partitions);
        }
    }

    public record MetadataReply(
            Error error,
            UUID clusterId,
            long appliedOffset,
            List<BrokerInfo> brokers,
            List<TopicInfo> topics)
            implements Protocol.Response {
        public MetadataReply {
            Objects.requireNonNull(error);
            cluster(clusterId);
            brokers = List.copyOf(brokers);
            topics = List.copyOf(topics);
            if (appliedOffset < 0) throw invalid("Negative metadata offset");
        }
    }

    public record CreateTopicReply(Error error, UUID topicId, long commitOffset)
            implements Protocol.Response {
        public CreateTopicReply {
            Objects.requireNonNull(error);
            cluster(topicId);
            if (commitOffset <= 0) throw invalid("Invalid committed topic offset");
        }
    }

    /** REJECTED proves that append never started; UNKNOWN must never be retried automatically. */
    public record ProduceResult(
            TopicPartition partition,
            Error error,
            Outcome outcome,
            long firstOffset,
            long nextOffset) {
        public ProduceResult {
            validPartition(partition);
            Objects.requireNonNull(error);
            Objects.requireNonNull(outcome);
            if (outcome == Outcome.SUCCESS) {
                if (error.code() != ErrorCode.NONE || firstOffset < 0 || nextOffset <= firstOffset)
                    throw invalid("Invalid Produce success");
            } else if (error.code() == ErrorCode.NONE || firstOffset != -1 || nextOffset != -1)
                throw invalid("Invalid Produce failure");
        }

        public boolean retrySafe() {
            return outcome == Outcome.REJECTED;
        }
    }

    public record ProduceReply(Error error, List<ProduceResult> results)
            implements Protocol.Response {
        public ProduceReply {
            Objects.requireNonNull(error);
            results = List.copyOf(results);
        }
    }

    /** start, LEO, HW and returned batches are captured by the same ordered partition read. */
    public record FetchResult(
            TopicPartition partition,
            Error error,
            long logStartOffset,
            long logEndOffset,
            long highWatermark,
            List<FetchBatch> batches) {
        public FetchResult {
            validPartition(partition);
            Objects.requireNonNull(error);
            batches = List.copyOf(batches);
            if (error.code() != ErrorCode.NONE) {
                if (logStartOffset != -1
                        || logEndOffset != -1
                        || highWatermark != -1
                        || !batches.isEmpty()) throw invalid("Invalid Fetch failure");
            } else {
                if (logStartOffset < 0
                        || highWatermark < logStartOffset
                        || logEndOffset < highWatermark) throw invalid("Invalid Fetch boundaries");
                long previous = -1;
                for (var batch : batches) {
                    long end = Math.addExact(batch.baseOffset(), batch.batch().records().size());
                    if (batch.baseOffset() < logStartOffset
                            || batch.baseOffset() < previous
                            || end > highWatermark
                            || batch.batch().records().isEmpty())
                        throw invalid("Batch outside readable prefix");
                    previous = end;
                }
            }
        }
    }

    public record FetchReply(Error error, List<FetchResult> results) implements Protocol.Response {
        public FetchReply {
            Objects.requireNonNull(error);
            results = List.copyOf(results);
        }
    }

    private static void validPartition(TopicPartition partition) {
        Objects.requireNonNull(partition);
        cluster(partition.topicId());
        if (partition.partition() < 0) throw invalid("Negative partition");
    }

    private static void cluster(UUID id) {
        Objects.requireNonNull(id);
        if (UNPINNED.equals(id)) throw invalid("Unpinned identity");
    }

    private static void timeout(int value) {
        if (value <= 0 || value > 30_000) throw invalid("Invalid request timeout");
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }
}
