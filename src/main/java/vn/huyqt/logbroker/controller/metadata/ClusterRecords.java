package vn.huyqt.logbroker.controller.metadata;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

/** Validated, immutable payloads of metadata schema v2. IDs have role-specific namespaces. */
public final class ClusterRecords {
    private ClusterRecords() {}

    public record Endpoint(String host, int port) {
        public Endpoint {
            Objects.requireNonNull(host);
            byte[] utf8 = host.getBytes(StandardCharsets.UTF_8);
            if (host.isBlank() || utf8.length > 255 || port < 1 || port > 65535
                    || !new String(utf8, StandardCharsets.UTF_8).equals(host))
                throw new IllegalArgumentException("Invalid advertised endpoint");
        }
    }

    /** Storage identity survives restart; incarnation identifies exactly one process. */
    public record Session(int brokerId, UUID storageId, UUID incarnationId, long brokerEpoch) {
        public Session {
            requireId(storageId);
            requireId(incarnationId);
            if (brokerId < 0 || brokerEpoch < 0) throw new IllegalArgumentException("Invalid session");
        }
    }

    public record FeatureLevel(short level) {
        public FeatureLevel {
            if (level != 2) throw new IllegalArgumentException("Unsupported metadata feature level");
        }
    }

    public record BrokerRegistration(Session session, Endpoint endpoint, short minVersion, short maxVersion) {
        public BrokerRegistration {
            Objects.requireNonNull(session);
            Objects.requireNonNull(endpoint);
            if (minVersion < 1 || maxVersion < minVersion)
                throw new IllegalArgumentException("Invalid feature range");
        }
    }

    /** Fencing is committed metadata; heartbeat timeout alone never changes this record locally. */
    public record BrokerState(int brokerId, long brokerEpoch, boolean fenced) {
        public BrokerState {
            if (brokerId < 0 || brokerEpoch < 0) throw new IllegalArgumentException("Invalid broker state");
        }
    }

    public record TopicRecord(UUID topicId, String name, int partitions) {
        public TopicRecord {
            requireId(topicId);
            // Preserve the data protocol's topic naming and partition limits.
            new TopicCreated(topicId, name, partitions);
        }
    }

    /** RF=1 assignment; leaderEpoch changes on a leader grant, partitionEpoch on every change. */
    public record PartitionRecord(UUID topicId, int partitionId, List<Integer> replicas,
                                  int leaderId, long leaderEpoch, long partitionEpoch) {
        public PartitionRecord {
            requireId(topicId);
            replicas = List.copyOf(replicas);
            if (partitionId < 0 || replicas.size() != 1 || replicas.getFirst() < 0
                    || leaderId != replicas.getFirst() || leaderEpoch < 0 || partitionEpoch < leaderEpoch)
                throw new IllegalArgumentException("Invalid RF=1 partition assignment");
        }
    }

    private static void requireId(UUID id) {
        Objects.requireNonNull(id);
        if (id.getMostSignificantBits() == 0 && id.getLeastSignificantBits() == 0)
            throw new IllegalArgumentException("Zero identity");
    }
}
