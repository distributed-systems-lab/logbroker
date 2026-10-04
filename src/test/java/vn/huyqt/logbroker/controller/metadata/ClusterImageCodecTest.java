package vn.huyqt.logbroker.controller.metadata;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.log.*;

class ClusterImageCodecTest {
    @Test
    void roundTripPreservesFeaturesSessionAssignmentsAndRevision() throws Exception {
        var state = ClusterMetadataApplyTest.initialized();
        state.apply(
                new QuorumBatch(
                        2,
                        List.of(
                                ClusterMetadataApplyTest.topic(1),
                                ClusterMetadataApplyTest.partition(0, 1, 2))));
        byte[] bytes = MetadataImageCodec.encodeV2(state.image(), MetadataLimits.defaults());
        assertEquals(bytes.length, MetadataImageCodec.encodedSize(state.image()));
        assertEquals(state.image(), MetadataImageCodec.decodeV2(bytes, MetadataLimits.defaults()));
        assertThrows(
                IOException.class,
                () ->
                        MetadataImageCodec.decodeV2(
                                Arrays.copyOf(bytes, bytes.length + 1), MetadataLimits.defaults()));
    }

    @Test
    void rejectsCountBeforeAllocationAndMissingAssignments() throws Exception {
        var state = ClusterMetadataApplyTest.initialized();
        byte[] bytes = MetadataImageCodec.encodeV2(state.image(), MetadataLimits.defaults());
        ByteBuffer.wrap(bytes).putInt(16, Integer.MAX_VALUE);
        assertThrows(
                IOException.class,
                () -> MetadataImageCodec.decodeV2(bytes, MetadataLimits.defaults()));
    }

    @Test
    void configuredImageBeyondOneChunkRoundTrips() throws Exception {
        var limits = new MetadataLimits(32, 1024, 8192, 4 * 1024 * 1024);
        var state = new MetadataStateMachine(limits, (short) 2);
        state.apply(ClusterMetadataApplyTest.featureBatch());
        state.apply(new QuorumBatch(1, List.of(ClusterMetadataApplyTest.registration(2))));
        for (int i = 1; i <= 1024; i++) {
            UUID id = new UUID(0, i);
            var entries = new ArrayList<QuorumEntry>();
            entries.add(
                    new QuorumEntry.TopicRecord(
                            0, new ClusterRecords.TopicRecord(id, "t" + i + "x".repeat(240), 8)));
            for (int p = 0; p < 8; p++)
                entries.add(
                        new QuorumEntry.PartitionRecord(
                                0, new ClusterRecords.PartitionRecord(id, p, List.of(1), 1, 0, 0)));
            state.apply(new QuorumBatch(state.image().appliedOffset(), entries));
        }
        byte[] bytes = MetadataImageCodec.encodeV2(state.image(), limits);
        assertEquals(bytes.length, MetadataImageCodec.encodedSize(state.image()));
        assertTrue(bytes.length > 256 * 1024);
        assertEquals(state.image(), MetadataImageCodec.decodeV2(bytes, limits));
    }
}
