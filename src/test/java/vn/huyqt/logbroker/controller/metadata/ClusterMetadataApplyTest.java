package vn.huyqt.logbroker.controller.metadata;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.log.*;

import java.io.IOException;
import java.util.*;

class ClusterMetadataApplyTest {
    static final UUID TOPIC = new UUID(0, 7);

    static QuorumBatch featureBatch() {
        return new QuorumBatch(
                0,
                List.of(
                        new QuorumEntry.FeatureLevel(
                                0, new ClusterRecords.FeatureLevel((short) 2))));
    }

    static QuorumEntry.BrokerRegistration registration(long brokerEpoch) {
        return new QuorumEntry.BrokerRegistration(
                0,
                new ClusterRecords.BrokerRegistration(
                        new ClusterRecords.Session(1, new UUID(0, 1), new UUID(0, 2), brokerEpoch),
                        new ClusterRecords.Endpoint("localhost", 9092),
                        (short) 2,
                        (short) 2));
    }

    static MetadataStateMachine initialized() throws IOException {
        var state = new MetadataStateMachine(MetadataLimits.defaults(), (short) 2);
        state.apply(featureBatch());
        state.apply(new QuorumBatch(1, List.of(registration(2))));
        return state;
    }

    static QuorumEntry.PartitionRecord partition(int id, long leaderEpoch, long partitionEpoch) {
        return new QuorumEntry.PartitionRecord(
                0,
                new ClusterRecords.PartitionRecord(
                        TOPIC, id, List.of(1), 1, leaderEpoch, partitionEpoch));
    }

    static QuorumEntry.TopicRecord topic(int count) {
        return new QuorumEntry.TopicRecord(
                0, new ClusterRecords.TopicRecord(TOPIC, "orders", count));
    }

    @Test
    void incompleteTopicBatchPublishesNothing() throws Exception {
        var state = initialized();
        var before = state.image();
        assertThrows(
                IOException.class,
                () -> state.apply(new QuorumBatch(2, List.of(topic(2), partition(0, 0, 0)))));
        assertSame(before, state.image());
        assertNull(state.find("orders"));
    }

    @Test
    void completeBatchPublishesAllAssignmentsAndLifecycleRevision() throws Exception {
        var state = initialized();
        assertTrue(state.image().brokers().get(1).fenced());
        state.apply(
                new QuorumBatch(
                        2,
                        List.of(
                                new QuorumEntry.BrokerState(
                                        0, new ClusterRecords.BrokerState(1, 2, false)),
                                topic(2),
                                partition(0, 0, 0),
                                partition(1, 0, 0))));
        assertEquals(6, state.image().appliedOffset());
        assertEquals(2, state.image().partitions().size());
        assertEquals(3, state.image().brokers().get(1).stateOffset());
        assertFalse(state.image().brokers().get(1).fenced());
        assertEquals(TOPIC, state.find("orders").id());
    }

    @Test
    void staleBrokerStateAndUnregisteredReplicaAreAtomicFailures() throws Exception {
        var state = initialized();
        var before = state.image();
        assertThrows(
                IOException.class,
                () ->
                        state.apply(
                                new QuorumBatch(
                                        2,
                                        List.of(
                                                new QuorumEntry.BrokerState(
                                                        0,
                                                        new ClusterRecords.BrokerState(
                                                                1, 1, false))))));
        var missing =
                new QuorumEntry.PartitionRecord(
                        0, new ClusterRecords.PartitionRecord(TOPIC, 0, List.of(2), 2, 0, 0));
        assertThrows(
                IOException.class,
                () -> state.apply(new QuorumBatch(2, List.of(topic(1), missing))));
        assertSame(before, state.image());
    }

    @Test
    void decreasingPartitionEpochAndConflictingTopicAreRejected() throws Exception {
        var state = initialized();
        state.apply(new QuorumBatch(2, List.of(topic(1), partition(0, 1, 2))));
        var before = state.image();
        assertThrows(
                IOException.class,
                () -> state.apply(new QuorumBatch(4, List.of(partition(0, 0, 1)))));
        assertThrows(IOException.class, () -> state.apply(new QuorumBatch(4, List.of(topic(2)))));
        assertSame(before, state.image());
    }

    @Test
    void featureMustPrecedeClusterMutationsAndBePreservedOnRestore() throws Exception {
        var state = new MetadataStateMachine(MetadataLimits.defaults(), (short) 2);
        assertThrows(
                IOException.class, () -> state.apply(new QuorumBatch(0, List.of(registration(1)))));
        var source = initialized();
        state.restore(source.image());
        assertEquals((short) 2, state.image().metadataVersion());
        assertEquals(source.image(), state.image());
    }

    @Test
    void storageIdentityCannotChangeAndBrokerEpochMustMatchRecordOffset() throws Exception {
        var state = initialized();
        assertThrows(
                IOException.class, () -> state.apply(new QuorumBatch(2, List.of(registration(2)))));
        var other =
                new QuorumEntry.BrokerRegistration(
                        0,
                        new ClusterRecords.BrokerRegistration(
                                new ClusterRecords.Session(1, new UUID(0, 99), new UUID(0, 3), 3),
                                new ClusterRecords.Endpoint("localhost", 9092),
                                (short) 2,
                                (short) 2));
        assertThrows(IOException.class, () -> state.apply(new QuorumBatch(2, List.of(other))));
    }
}
