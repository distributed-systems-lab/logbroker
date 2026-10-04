package vn.huyqt.logbroker.controller.log;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.metadata.ClusterRecords;
import vn.huyqt.logbroker.controller.metadata.MetadataLimits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

class ClusterEntryCodecTest {
    private static final UUID STORAGE = new UUID(0, 1);
    private static final UUID INCARNATION = new UUID(0, 2);
    private static final UUID TOPIC = new UUID(0, 3);

    @Test
    void featureVectorHasStableBytes() throws Exception {
        var entry = new QuorumEntry.FeatureLevel(0, new ClusterRecords.FeatureLevel((short) 2));
        byte[] golden = HexFormat.of().parseHex("00020400000000000000000002");
        assertArrayEquals(golden, QuorumEntryCodec.encode(entry, (short) 2));
        assertEquals(entry, QuorumEntryCodec.decode(golden, MetadataLimits.defaults()));
    }

    @Test
    void registrationPreservesDistinctStorageAndIncarnation() throws Exception {
        var entry =
                new QuorumEntry.BrokerRegistration(
                        7,
                        new ClusterRecords.BrokerRegistration(
                                new ClusterRecords.Session(4, STORAGE, INCARNATION, 19),
                                new ClusterRecords.Endpoint("máy.local", 9092),
                                (short) 2,
                                (short) 2));
        assertEquals(entry, QuorumEntryCodec.decode(QuorumEntryCodec.encode(entry)));
        assertEquals(QuorumEntryCodec.encodedSize(entry), QuorumEntryCodec.encode(entry).length);
    }

    @Test
    void checkedInVectorsDecodeAndReencodeExactly() throws Exception {
        try (var stream =
                getClass().getResourceAsStream("/controller/protocol-v2-entry-vectors.txt")) {
            assertNotNull(stream);
            for (var line :
                    new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                            .lines()
                            .toList()) {
                if (line.isBlank() || line.startsWith("#")) continue;
                byte[] golden = HexFormat.of().parseHex(line.split(" = ")[1]);
                assertArrayEquals(golden, QuorumEntryCodec.encode(QuorumEntryCodec.decode(golden)));
            }
        }
    }

    @Test
    void malformedUtf8HostAndTruncatedRegistrationAreRejected() {
        var entry =
                new QuorumEntry.BrokerRegistration(
                        0,
                        new ClusterRecords.BrokerRegistration(
                                new ClusterRecords.Session(4, STORAGE, INCARNATION, 19),
                                new ClusterRecords.Endpoint("local", 9092),
                                (short) 2,
                                (short) 2));
        byte[] bytes = QuorumEntryCodec.encode(entry);
        bytes[59] = (byte) 0xff;
        assertThrows(IOException.class, () -> QuorumEntryCodec.decode(bytes));
        assertThrows(IOException.class, () -> QuorumEntryCodec.decode(Arrays.copyOf(bytes, 58)));
    }

    @Test
    void partitionEpochAndLeaderEpochRoundTripIndependently() throws Exception {
        var entry =
                new QuorumEntry.PartitionRecord(
                        7, new ClusterRecords.PartitionRecord(TOPIC, 2, List.of(4), 4, 3, 9));
        assertEquals(entry, QuorumEntryCodec.decode(QuorumEntryCodec.encode(entry)));
    }

    @Test
    void allRecordSizesMatchEncoding() throws Exception {
        for (var entry :
                List.of(
                        new QuorumEntry.LeaderChange(7, 1),
                        new QuorumEntry.ReadBarrier(7),
                        new QuorumEntry.FeatureLevel(7, new ClusterRecords.FeatureLevel((short) 2)),
                        new QuorumEntry.BrokerState(7, new ClusterRecords.BrokerState(4, 19, true)),
                        new QuorumEntry.TopicRecord(
                                7, new ClusterRecords.TopicRecord(TOPIC, "orders", 2)),
                        new QuorumEntry.PartitionRecord(
                                7,
                                new ClusterRecords.PartitionRecord(
                                        TOPIC, 0, List.of(4), 4, 0, 0)))) {
            byte[] bytes = QuorumEntryCodec.encode(entry, (short) 2);
            assertEquals(QuorumEntryCodec.encodedSize(entry), bytes.length);
            assertEquals(entry, QuorumEntryCodec.decode(bytes));
            assertThrows(
                    IOException.class,
                    () -> QuorumEntryCodec.decode(Arrays.copyOf(bytes, bytes.length - 1)));
            assertThrows(
                    IOException.class,
                    () -> QuorumEntryCodec.decode(Arrays.copyOf(bytes, bytes.length + 1)));
        }
    }

    @Test
    void rejectsMalformedVersionKindBooleanAndReplicaCount() {
        byte[] state =
                QuorumEntryCodec.encode(
                        new QuorumEntry.BrokerState(
                                0, new ClusterRecords.BrokerState(4, 19, true)));
        state[state.length - 1] = 2;
        assertThrows(IOException.class, () -> QuorumEntryCodec.decode(state));
        byte[] partition =
                QuorumEntryCodec.encode(
                        new QuorumEntry.PartitionRecord(
                                0,
                                new ClusterRecords.PartitionRecord(TOPIC, 0, List.of(4), 4, 0, 0)));
        ByteBuffer.wrap(partition).putInt(31, Integer.MAX_VALUE);
        assertThrows(IOException.class, () -> QuorumEntryCodec.decode(partition));
        assertThrows(IOException.class, () -> QuorumEntryCodec.decode(new byte[1024 * 1024 + 1]));
        for (String hex : List.of("00030400000000000000000002", "00027f0000000000000000"))
            assertThrows(
                    IOException.class, () -> QuorumEntryCodec.decode(HexFormat.of().parseHex(hex)));
    }

    @Test
    void validatesIdentitiesEndpointsEpochsAndFeatureRanges() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClusterRecords.Session(1, new UUID(0, 0), INCARNATION, 1));
        assertThrows(
                IllegalArgumentException.class, () -> new ClusterRecords.Endpoint("\ud800", 9092));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClusterRecords.Endpoint("x".repeat(256), 9092));
        assertThrows(IllegalArgumentException.class, () -> new ClusterRecords.Endpoint("local", 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClusterRecords.PartitionRecord(TOPIC, 0, List.of(1, 2), 1, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClusterRecords.PartitionRecord(TOPIC, 0, List.of(1), 2, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ClusterRecords.BrokerRegistration(
                                new ClusterRecords.Session(1, STORAGE, INCARNATION, 1),
                                new ClusterRecords.Endpoint("localhost", 9092),
                                (short) 3,
                                (short) 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> new QuorumEntry.FeatureLevel(-1, new ClusterRecords.FeatureLevel((short) 2)));
    }

    @Test
    void metadataLimitsPreflightImageCapacity() {
        var limits = MetadataLimits.defaults();
        assertTrue(limits.maxEncodedImageBytes() <= limits.maxImageBytes());
        assertThrows(IllegalArgumentException.class, () -> new MetadataLimits(32, 128, 1024, 100));
        assertThrows(
                IllegalArgumentException.class,
                () -> new MetadataLimits(Integer.MAX_VALUE, 128, 1024, 64 * 1024 * 1024));
    }
}
