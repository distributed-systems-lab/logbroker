package vn.huyqt.logbroker.protocol;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ClusterProtocolCodecTest {
    @Test
    void unknownIsNotRetrySafeAndImpossibleSuccessIsRejected() {
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var timeout = new Protocol.Error(ErrorCode.REQUEST_TIMED_OUT, "deadline");
        assertFalse(
                new ClusterProtocol.ProduceResult(
                                tp, timeout, ClusterProtocol.Outcome.UNKNOWN, -1, -1)
                        .retrySafe());
        assertTrue(
                new ClusterProtocol.ProduceResult(
                                tp, timeout, ClusterProtocol.Outcome.REJECTED, -1, -1)
                        .retrySafe());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ClusterProtocol.ProduceResult(
                                tp, timeout, ClusterProtocol.Outcome.SUCCESS, 0, 1));
    }

    @Test
    void bootstrapMetadataHasIndependentStableVector() throws Exception {
        var frame =
                new Protocol.RequestFrame(
                        (short) 2,
                        (short) 2,
                        9,
                        new ClusterProtocol.Metadata(new UUID(0, 0), List.of()));
        String vector;
        try (var input = getClass().getResourceAsStream("/protocol/cluster-v2-vectors.txt")) {
            vector =
                    new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                            .lines()
                            .filter(line -> line.startsWith("metadata-bootstrap="))
                            .findFirst()
                            .orElseThrow()
                            .split("=", 2)[1];
        }
        byte[] expected = HexFormat.of().parseHex(vector);
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        assertArrayEquals(expected, codec.encodeRequest(frame));
        assertEquals(frame, codec.decodeRequest(expected));
    }

    @Test
    void routedRequestsAndRepliesRoundTripWithDistinctEpochs() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var route =
                new ClusterProtocol.Route(
                        new Protocol.TopicPartition(new UUID(0, 7), 0), 3, 11, 17);
        var request =
                new Protocol.RequestFrame(
                        (short) 4,
                        (short) 2,
                        1,
                        new ClusterProtocol.Fetch(
                                new UUID(0, 1),
                                4096,
                                0,
                                100,
                                List.of(new ClusterProtocol.FetchEntry(route, 3, 2048)),
                                true));
        assertEquals(request, codec.decodeRequest(codec.encodeRequest(request)));
        var reply =
                new Protocol.ResponseFrame(
                        (short) 4,
                        (short) 2,
                        1,
                        new ClusterProtocol.FetchReply(
                                Protocol.Error.none(),
                                List.of(
                                        new ClusterProtocol.FetchResult(
                                                route.partition(),
                                                Protocol.Error.none(),
                                                0,
                                                9,
                                                7,
                                                List.of()))));
        assertEquals(reply, codec.decodeResponse(codec.encodeResponse(reply)));
    }

    @Test
    void truncationTrailingBytesAndInvalidOversizeFlagFailClosed() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var route =
                new ClusterProtocol.Route(
                        new Protocol.TopicPartition(new UUID(0, 7), 0), 3, 11, 17);
        byte[] bytes =
                codec.encodeRequest(
                        new Protocol.RequestFrame(
                                (short) 4,
                                (short) 2,
                                1,
                                new ClusterProtocol.Fetch(
                                        new UUID(0, 1),
                                        4096,
                                        0,
                                        100,
                                        List.of(new ClusterProtocol.FetchEntry(route, 3, 2048)),
                                        true)));
        bytes[bytes.length - 1] = 2;
        assertThrows(ProtocolException.class, () -> codec.decodeRequest(bytes));
        for (int i = 0; i < bytes.length; i++) {
            byte[] cut = java.util.Arrays.copyOf(bytes, i);
            assertThrows(ProtocolException.class, () -> codec.decodeRequest(cut));
        }
    }

    @Test
    void malformedOutcomeAndImpossibleOffsetsAreRejected() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        byte[] bytes =
                codec.encodeResponse(
                        new Protocol.ResponseFrame(
                                (short) 3,
                                (short) 2,
                                1,
                                new ClusterProtocol.ProduceReply(
                                        Protocol.Error.none(),
                                        List.of(
                                                new ClusterProtocol.ProduceResult(
                                                        tp,
                                                        new Protocol.Error(
                                                                ErrorCode.REQUEST_TIMED_OUT, ""),
                                                        ClusterProtocol.Outcome.UNKNOWN,
                                                        -1,
                                                        -1)))));
        // envelope16, top error6, count4, partition20, entry error6, outcome at52.
        bytes[52] = 3;
        assertThrows(ProtocolException.class, () -> codec.decodeResponse(bytes));
        bytes[52] = 0;
        assertThrows(ProtocolException.class, () -> codec.decodeResponse(bytes));
    }

    @Test
    void metadataLimitsAreConfigurableAndCheckedBeforeAllocation() throws Exception {
        var codec =
                new ProtocolCodec(
                        ProtocolLimits.defaults(),
                        new vn.huyqt.logbroker.controller.metadata.MetadataLimits(
                                1, 1, 1, 64 * 1024 * 1024));
        byte[] bytes =
                new ProtocolCodec(ProtocolLimits.defaults())
                        .encodeRequest(
                                new Protocol.RequestFrame(
                                        (short) 2,
                                        (short) 2,
                                        1,
                                        new ClusterProtocol.Metadata(
                                                ClusterProtocol.UNPINNED, List.of("a", "b"))));
        assertThrows(ProtocolException.class, () -> codec.decodeRequest(bytes));
    }
}
