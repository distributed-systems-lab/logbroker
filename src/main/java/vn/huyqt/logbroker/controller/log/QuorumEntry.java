package vn.huyqt.logbroker.controller.log;

import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords;

import java.util.Objects;

/**
 * One record of the metadata quorum log. Wire and storage encoding is defined by {@link
 * QuorumEntryCodec} and {@code docs/controller-protocol-v1.md}.
 *
 * <p>Control entries occupy offsets and count toward snapshot boundaries and epochs, but do not
 * change the topic catalog.
 */
public sealed interface QuorumEntry {
    /** Epoch of the leader that appended this entry. */
    long epoch();

    /**
     * Control entry a new leader appends first in its epoch. The leader is not ready for admin work
     * until this entry is committed and applied.
     */
    record LeaderChange(long epoch, int leaderId) implements QuorumEntry {
        public LeaderChange {
            if (epoch < 0 || leaderId < 0)
                throw new IllegalArgumentException("Invalid leader change");
        }
    }

    /** Metadata entry that creates a topic when applied. */
    record Topic(long epoch, TopicCreated event) implements QuorumEntry {
        public Topic {
            if (epoch < 0) throw new IllegalArgumentException("Negative epoch");
            Objects.requireNonNull(event);
        }
    }

    /**
     * Control entry for linearizable reads: reads admitted before it is appended complete once it
     * is committed and applied.
     */
    record ReadBarrier(long epoch) implements QuorumEntry {
        public ReadBarrier {
            if (epoch < 0) throw new IllegalArgumentException("Negative epoch");
        }
    }

    record FeatureLevel(long epoch, ClusterRecords.FeatureLevel event) implements QuorumEntry {
        public FeatureLevel {
            validate(epoch, event);
        }
    }

    record BrokerRegistration(long epoch, ClusterRecords.BrokerRegistration event)
            implements QuorumEntry {
        public BrokerRegistration {
            validate(epoch, event);
        }
    }

    record BrokerState(long epoch, ClusterRecords.BrokerState event) implements QuorumEntry {
        public BrokerState {
            validate(epoch, event);
        }
    }

    record TopicRecord(long epoch, ClusterRecords.TopicRecord event) implements QuorumEntry {
        public TopicRecord {
            validate(epoch, event);
        }
    }

    record PartitionRecord(long epoch, ClusterRecords.PartitionRecord event)
            implements QuorumEntry {
        public PartitionRecord {
            validate(epoch, event);
        }
    }

    private static void validate(long epoch, Object event) {
        if (epoch < 0) throw new IllegalArgumentException("Negative epoch");
        Objects.requireNonNull(event);
    }
}
