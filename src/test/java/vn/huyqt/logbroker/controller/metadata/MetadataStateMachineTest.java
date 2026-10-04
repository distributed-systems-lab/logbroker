package vn.huyqt.logbroker.controller.metadata;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.log.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class MetadataStateMachineTest {
    @Test
    void controlsAdvanceApplyWithoutCreatingTopics() throws Exception {
        var state = new MetadataStateMachine();
        state.apply(new QuorumBatch(0, List.of(new QuorumEntry.LeaderChange(1, 0))));
        state.apply(new QuorumBatch(1, List.of(new QuorumEntry.ReadBarrier(1))));
        assertEquals(2, state.image().appliedOffset());
        assertTrue(state.image().topics().isEmpty());
    }

    @Test
    void rejectsGapAndConflictingTopicWithoutPartiallyApplyingBatch() throws Exception {
        var state = new MetadataStateMachine();
        var topic = new TopicCreated(new UUID(0, 1), "orders", 3);
        state.apply(new QuorumBatch(0, List.of(new QuorumEntry.Topic(1, topic))));
        assertThrows(
                IOException.class,
                () -> state.apply(new QuorumBatch(2, List.of(new QuorumEntry.ReadBarrier(1)))));
        assertThrows(
                IOException.class,
                () ->
                        state.apply(
                                new QuorumBatch(
                                        1,
                                        List.of(
                                                new QuorumEntry.Topic(
                                                        1,
                                                        new TopicCreated(new UUID(0, 2), "new", 1)),
                                                new QuorumEntry.Topic(
                                                        1,
                                                        new TopicCreated(
                                                                new UUID(0, 3), "orders", 3))))));
        assertEquals(1, state.image().appliedOffset());
        assertNull(state.find("new"));
        state.apply(new QuorumBatch(1, List.of(new QuorumEntry.Topic(1, topic))));
        assertEquals(2, state.image().appliedOffset());
    }

    @Test
    void snapshotRoundTripPreservesIdsAndIsIndependentOfMutableInput() throws Exception {
        var topics = new ArrayList<TopicCreated>();
        topics.add(new TopicCreated(new UUID(0, 9), "z", 1));
        topics.add(new TopicCreated(new UUID(0, 8), "a", 2));
        var image = new MetadataImage(17, topics);
        topics.clear();
        var restored = MetadataImageCodec.decode(MetadataImageCodec.encode(image));
        assertEquals(image, restored);
        assertEquals("a", restored.topics().getFirst().name());
        var state = new MetadataStateMachine();
        state.restore(restored);
        assertEquals(new UUID(0, 9), state.find("z").id());
    }

    @Test
    void snapshotRejectsTrailingBytesAndImpossibleCount() throws Exception {
        byte[] empty = MetadataImageCodec.encode(new MetadataImage(0, List.of()));
        assertThrows(
                IOException.class,
                () -> MetadataImageCodec.decode(java.util.Arrays.copyOf(empty, empty.length + 1)));
        java.nio.ByteBuffer.wrap(empty).putInt(8, Integer.MAX_VALUE);
        assertThrows(IOException.class, () -> MetadataImageCodec.decode(empty));
    }
}
