package vn.huyqt.logbroker.controller.metadata;

import java.util.*;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

/** Immutable metadata at an exclusive committed batch boundary. Maps and lists are copied. */
public record MetadataImage(
        long appliedOffset,
        List<TopicCreated> topics,
        short metadataVersion,
        Map<Integer, BrokerRegistrationView> brokers,
        Map<PartitionKey, ClusterRecords.PartitionRecord> partitions) {
    public record PartitionKey(UUID topicId, int partitionId) implements Comparable<PartitionKey> {
        public PartitionKey {
            Objects.requireNonNull(topicId);
            if (partitionId < 0) throw new IllegalArgumentException("Negative partition");
        }

        public int compareTo(PartitionKey other) {
            int c =
                    Long.compareUnsigned(
                            topicId.getMostSignificantBits(),
                            other.topicId.getMostSignificantBits());
            if (c == 0)
                c =
                        Long.compareUnsigned(
                                topicId.getLeastSignificantBits(),
                                other.topicId.getLeastSignificantBits());
            return c == 0 ? Integer.compare(partitionId, other.partitionId) : c;
        }
    }

    /** stateOffset is the exclusive offset of the latest registration/fence/unfence revision. */
    public record BrokerRegistrationView(
            ClusterRecords.BrokerRegistration registration, boolean fenced, long stateOffset) {
        public BrokerRegistrationView {
            Objects.requireNonNull(registration);
            if (stateOffset < registration.session().brokerEpoch())
                throw new IllegalArgumentException("Invalid lifecycle revision");
        }
    }

    public MetadataImage {
        if (appliedOffset < 0 || metadataVersion < 0 || metadataVersion > 2)
            throw new IllegalArgumentException("Invalid image boundary or feature level");
        topics = topics.stream().sorted(Comparator.comparing(TopicCreated::name)).toList();
        brokers = Collections.unmodifiableMap(new TreeMap<>(brokers));
        partitions = Collections.unmodifiableMap(new TreeMap<>(partitions));
        if (topics.stream().map(TopicCreated::name).distinct().count() != topics.size()
                || topics.stream().map(TopicCreated::id).distinct().count() != topics.size())
            throw new IllegalArgumentException("Conflicting topic identities");
        if (metadataVersion == 1
                && (topics.size() > 128
                        || topics.stream().mapToLong(TopicCreated::partitions).sum() > 1024
                        || !brokers.isEmpty()
                        || !partitions.isEmpty()))
            throw new IllegalArgumentException("Invalid v1 image");
        if (metadataVersion == 0
                && (!topics.isEmpty() || !brokers.isEmpty() || !partitions.isEmpty()))
            throw new IllegalArgumentException("Uninitialized cluster metadata");
    }

    /** Legacy image constructor for v1 readers and fixtures. */
    public MetadataImage(long appliedOffset, List<TopicCreated> topics) {
        this(appliedOffset, topics, (short) 1, Map.of(), Map.of());
    }

    /** V2 starts at feature level zero until its FeatureLevel record applies. */
    public static MetadataImage empty(short schema) {
        if (schema != 1 && schema != 2)
            throw new IllegalArgumentException("Unsupported image schema");
        return new MetadataImage(0, List.of(), (short) (schema == 1 ? 1 : 0), Map.of(), Map.of());
    }

    /** Validates capacity and complete cross-references before publication. */
    public void validate(MetadataLimits limits) {
        if (topics.size() > limits.maxTopics()
                || brokers.size() > limits.maxBrokers()
                || topics.stream().mapToLong(TopicCreated::partitions).sum()
                        > limits.maxPartitions()
                || partitions.size() > limits.maxPartitions())
            throw new IllegalArgumentException("Metadata capacity exceeded");
        for (var broker : brokers.entrySet()) {
            var view = broker.getValue();
            var r = view.registration();
            if (broker.getKey() != r.session().brokerId()
                    || view.stateOffset() > appliedOffset
                    || r.minVersion() > metadataVersion
                    || r.maxVersion() < metadataVersion)
                throw new IllegalArgumentException("Invalid broker image");
        }
        if (metadataVersion != 2) return;
        long expected = topics.stream().mapToLong(TopicCreated::partitions).sum();
        if (partitions.size() != expected)
            throw new IllegalArgumentException("Incomplete assignments");
        var byId = new HashMap<UUID, TopicCreated>();
        topics.forEach(t -> byId.put(t.id(), t));
        for (var entry : partitions.entrySet()) {
            var p = entry.getValue();
            var topic = byId.get(p.topicId());
            if (!entry.getKey().equals(new PartitionKey(p.topicId(), p.partitionId()))
                    || topic == null
                    || p.partitionId() >= topic.partitions()
                    || !brokers.containsKey(p.leaderId()))
                throw new IllegalArgumentException("Invalid assignment reference");
        }
    }
}
