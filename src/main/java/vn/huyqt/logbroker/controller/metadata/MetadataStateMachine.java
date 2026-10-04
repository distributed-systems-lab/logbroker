package vn.huyqt.logbroker.controller.metadata;

import vn.huyqt.logbroker.broker.metadata.TopicCatalog;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.log.*;

import java.io.IOException;
import java.util.*;

/**
 * Consensus supplies only committed batches; applying a bad batch publishes no partial image.
 *
 * <p>Holds the applied {@link MetadataImage} and a matching {@link TopicCatalog}. Batches must be
 * applied in log order without gaps; control entries advance the applied offset without changing
 * topics. Not thread-safe; the caller serializes access.
 */
public final class MetadataStateMachine {
    private MetadataImage image;
    private final MetadataLimits limits;
    private final short schema;
    private TopicCatalog catalog = new TopicCatalog();

    public MetadataStateMachine() {
        this(MetadataLimits.defaults(), (short) 1);
    }

    public MetadataStateMachine(MetadataLimits limits, short version) {
        this.limits = Objects.requireNonNull(limits);
        schema = version;
        image = MetadataImage.empty(version);
    }

    /**
     * Applies one committed batch and advances the image to its end.
     *
     * @throws IOException if the batch does not start at the applied offset, violates a metadata
     *     record invariant, or exceeds the image limits; the previous image stays published
     */
    public void apply(QuorumBatch batch) throws IOException {
        if (batch.baseOffset() != image.appliedOffset()) throw new IOException("Apply gap");
        // Build into a fresh catalog so a failure part-way through leaves the current state intact.
        var next = new TopicCatalog();
        for (var topic : image.topics()) next.apply(topic);
        var brokers = new TreeMap<>(image.brokers());
        var partitions = new TreeMap<>(image.partitions());
        short feature = image.metadataVersion();
        long offset = batch.baseOffset();
        try {
            for (var entry : batch.entries()) {
                offset++;
                if (entry instanceof QuorumEntry.LeaderChange
                        || entry instanceof QuorumEntry.ReadBarrier) continue;
                if (entry instanceof QuorumEntry.Topic topic) {
                    if (schema != 1) throw new IOException("Legacy topic in cluster history");
                    next.apply(topic.event());
                    continue;
                }
                if (schema != 2) throw new IOException("Cluster record in legacy history");
                if (entry instanceof QuorumEntry.FeatureLevel level) {
                    feature = level.event().level();
                    continue;
                }
                if (feature != 2) throw new IOException("Cluster feature not initialized");
                switch (entry) {
                    case QuorumEntry.BrokerRegistration registration -> {
                        var r = registration.event();
                        var session = r.session();
                        var old = brokers.get(session.brokerId());
                        if (session.brokerEpoch() != offset
                                || r.minVersion() > feature
                                || r.maxVersion() < feature
                                || old != null
                                        && (!old.registration()
                                                        .session()
                                                        .storageId()
                                                        .equals(session.storageId())
                                                || session.brokerEpoch()
                                                        <= old.registration()
                                                                .session()
                                                                .brokerEpoch()
                                                || !old.fenced()))
                            throw new IOException("Invalid registration history");
                        brokers.put(
                                session.brokerId(),
                                new MetadataImage.BrokerRegistrationView(r, true, offset));
                    }
                    case QuorumEntry.BrokerState state -> {
                        var s = state.event();
                        var old = brokers.get(s.brokerId());
                        if (old == null
                                || old.registration().session().brokerEpoch() != s.brokerEpoch())
                            throw new IOException("Stale lifecycle session");
                        brokers.put(
                                s.brokerId(),
                                new MetadataImage.BrokerRegistrationView(
                                        old.registration(), s.fenced(), offset));
                    }
                    case QuorumEntry.TopicRecord topic ->
                            next.apply(
                                    new TopicCreated(
                                            topic.event().topicId(),
                                            topic.event().name(),
                                            topic.event().partitions()));
                    case QuorumEntry.PartitionRecord partition -> {
                        var p = partition.event();
                        var key = new MetadataImage.PartitionKey(p.topicId(), p.partitionId());
                        var old = partitions.get(key);
                        if (old != null
                                && !old.equals(p)
                                && (p.partitionEpoch() != old.partitionEpoch() + 1
                                        || p.leaderEpoch() < old.leaderEpoch()
                                        || p.leaderEpoch() > old.leaderEpoch() + 1
                                        || p.leaderId() != old.leaderId()
                                                && p.leaderEpoch() != old.leaderEpoch() + 1))
                            throw new IOException("Invalid partition epoch transition");
                        partitions.put(key, p);
                    }
                    default -> throw new IOException("Invalid metadata entry");
                }
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid metadata batch", e);
        }
        var topics = new ArrayList<TopicCreated>();
        for (var topic : next.snapshot()) topics.add(next.find(topic.name()));
        try {
            var candidate =
                    new MetadataImage(batch.nextOffset(), topics, feature, brokers, partitions);
            candidate.validate(limits);
            image = candidate;
        } catch (IllegalArgumentException e) {
            throw new IOException("Metadata capacity exceeded", e);
        }
        catalog = next;
    }

    /** Replaces all state with {@code restored}, such as a snapshot image. */
    public void restore(MetadataImage restored) throws IOException {
        try {
            if (schema == 1 && restored.metadataVersion() != 1
                    || schema == 2 && restored.metadataVersion() == 1)
                throw new IllegalArgumentException("Snapshot schema mismatch");
            restored.validate(limits);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid restored image", e);
        }
        var next = new TopicCatalog();
        for (var topic : restored.topics()) next.apply(topic);
        image = restored;
        catalog = next;
    }

    public MetadataImage image() {
        return image;
    }

    /** Returns the applied topic named {@code name}, or {@code null} if there is none. */
    public TopicCreated find(String name) {
        return catalog.find(name);
    }
}
