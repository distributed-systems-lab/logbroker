package vn.huyqt.logbroker.protocol;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

class ProtocolCodecTest {
    @Test
    void preflightAccountsForDecodedRecordAndHeaderObjects() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var headers = java.util.Collections.nCopies(50, new RecordHeader("", null));
        var frame =
                new RequestFrame(
                        (short) 3,
                        (short) 1,
                        9,
                        new Produce(
                                AckMode.APPENDED,
                                1000,
                                List.of(
                                        new ProduceEntry(
                                                new TopicPartition(new UUID(1, 2), 0),
                                                new Batch(
                                                        List.of(
                                                                new LogRecord(
                                                                        0, null, null,
                                                                        headers)))))));
        byte[] bytes = codec.encodeRequest(frame);
        assertEquals(2L * bytes.length + 64L * 51, codec.estimatedDecodedBytes(bytes));
    }

    @Test
    void createTopicFrameHasStableBytes() throws Exception {
        var frame =
                new Protocol.RequestFrame(
                        (short) 1, (short) 1, 7, new Protocol.CreateTopic("x", 1));
        byte[] expected =
                HexFormat.of().parseHex("00000015000100010000000000000007000000017800000001");
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        assertArrayEquals(expected, codec.encodeRequest(frame));
        assertArrayEquals(fixture("create-topic-v1.hex"), codec.encodeRequest(frame));
        assertEquals(frame, codec.decodeRequest(expected));
    }

    @Test
    void refusesTrailingBodyBytes() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        byte[] frame =
                codec.encodeRequest(
                        new Protocol.RequestFrame(
                                (short) 1, (short) 1, 7, new Protocol.CreateTopic("x", 1)));
        byte[] trailing = java.util.Arrays.copyOf(frame, frame.length + 1);
        java.nio.ByteBuffer.wrap(trailing).putInt(trailing.length - 4);
        assertThrows(ProtocolException.class, () -> codec.decodeRequest(trailing));
    }

    @Test
    void metadataWithShortNameRoundTrips() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var request = new RequestFrame((short) 2, (short) 1, 8, new Metadata(List.of("x")));
        assertEquals(request, codec.decodeRequest(codec.encodeRequest(request)));
        var reply =
                new ResponseFrame(
                        (short) 2,
                        (short) 1,
                        8,
                        new MetadataReply(
                                Protocol.Error.none(),
                                "h",
                                9092,
                                List.of(
                                        new TopicInfo(
                                                "x",
                                                new UUID(1, 2),
                                                List.of(
                                                        new PartitionInfo(
                                                                0, Protocol.Error.none()))))));
        assertEquals(reply, codec.decodeResponse(codec.encodeResponse(reply)));
    }

    @Test
    void produceAndFetchRoundTripWithIndependentPartitionResults() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var tp = new TopicPartition(new UUID(1, 2), 0);
        var batch = new Batch(List.of(new LogRecord(7, null, new byte[0], List.of())));
        var produce =
                new RequestFrame(
                        (short) 3,
                        (short) 1,
                        9,
                        new Produce(AckMode.FLUSHED, 1000, List.of(new ProduceEntry(tp, batch))));
        assertEquals(produce, codec.decodeRequest(codec.encodeRequest(produce)));
        assertArrayEquals(fixture("produce-v1.hex"), codec.encodeRequest(produce));
        var produced =
                new ResponseFrame(
                        (short) 3,
                        (short) 1,
                        9,
                        new ProduceReply(
                                Protocol.Error.none(),
                                List.of(new ProduceResult(tp, Protocol.Error.none(), 0, 1))));
        assertEquals(produced, codec.decodeResponse(codec.encodeResponse(produced)));
        var fetch =
                new RequestFrame(
                        (short) 4,
                        (short) 1,
                        10,
                        new Fetch(1024, 1, 500, List.of(new FetchEntry(tp, 0, 1024))));
        assertEquals(fetch, codec.decodeRequest(codec.encodeRequest(fetch)));
        assertArrayEquals(fixture("fetch-v1.hex"), codec.encodeRequest(fetch));
        var fetched =
                new ResponseFrame(
                        (short) 4,
                        (short) 1,
                        10,
                        new FetchReply(
                                Protocol.Error.none(),
                                List.of(
                                        new FetchResult(
                                                tp,
                                                Protocol.Error.none(),
                                                0,
                                                1,
                                                List.of(new FetchBatch(0, batch))))));
        assertEquals(fetched, codec.decodeResponse(codec.encodeResponse(fetched)));
    }

    @Test
    void rejectsDuplicateFetchPartitionsAndUnknownOperation() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var tp = new TopicPartition(new UUID(1, 2), 0);
        var request =
                new RequestFrame(
                        (short) 4,
                        (short) 1,
                        7,
                        new Fetch(
                                1024,
                                0,
                                0,
                                List.of(new FetchEntry(tp, 0, 1024), new FetchEntry(tp, 1, 1024))));
        assertEquals(
                ErrorCode.INVALID_REQUEST,
                assertThrows(
                                ProtocolException.class,
                                () -> codec.decodeRequest(codec.encodeRequest(request)))
                        .code());
        byte[] unknown =
                codec.encodeRequest(
                        new RequestFrame((short) 1, (short) 1, 7, new CreateTopic("x", 1)));
        java.nio.ByteBuffer.wrap(unknown).putShort(4, (short) 99);
        assertEquals(
                ErrorCode.UNSUPPORTED_OPERATION,
                assertThrows(ProtocolException.class, () -> codec.decodeRequest(unknown)).code());
    }

    @Test
    void rejectsTruncatedFrameAndMalformedUtf8() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        byte[] frame =
                codec.encodeRequest(
                        new RequestFrame((short) 1, (short) 1, 7, new CreateTopic("x", 1)));
        assertThrows(
                ProtocolException.class,
                () -> codec.decodeRequest(java.util.Arrays.copyOf(frame, frame.length - 1)));
        frame[20] = (byte) 0xff;
        assertThrows(ProtocolException.class, () -> codec.decodeRequest(frame));
    }

    @Test
    void errorReplyEchoesUnknownOperationAndVersion() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        var frame =
                new ResponseFrame(
                        (short) 99,
                        (short) 8,
                        7,
                        new Failure(
                                new Protocol.Error(ErrorCode.UNSUPPORTED_OPERATION, "unknown")));
        assertEquals(frame, codec.decodeResponse(codec.encodeResponse(frame)));
    }

    private byte[] fixture(String name) throws Exception {
        try (var stream = getClass().getResourceAsStream("/protocol/" + name)) {
            assertNotNull(stream);
            return HexFormat.of()
                    .parseHex(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).trim());
        }
    }
}
