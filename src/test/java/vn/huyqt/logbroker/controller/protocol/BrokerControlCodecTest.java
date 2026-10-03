package vn.huyqt.logbroker.controller.protocol;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;

class BrokerControlCodecTest {
    private final ControllerConfig config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
    private final ClusterRecords.Session session = new ClusterRecords.Session(7, new UUID(0, 1), new UUID(0, 2), 9);

    @Test void independentDescribeVectorPreservesEnvelopeRoleAndCrc() throws Exception {
        try (var stream = getClass().getResourceAsStream("/controller/protocol-v2-vectors.txt")) {
            assertNotNull(stream);
            var line = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                .lines().filter(s -> !s.startsWith("#") && !s.isBlank()).findFirst().orElseThrow();
            byte[] golden = HexFormat.of().parseHex(line.split(" = ")[1]);
            var decoded = QuorumCodec.decode(golden, config);
            assertEquals(BrokerControlProtocol.SenderRole.BROKER, decoded.senderRole());
            assertEquals(7, decoded.requestId());
            assertArrayEquals(golden, QuorumCodec.encode(decoded));
            assertEquals(golden.length, QuorumCodec.encodedSize(decoded));
        }
    }

    @Test void registerCreateAndSnapshotRepliesHaveExactSizesAndChunkCrc() throws Exception {
        var meta = new ReplyMeta(QuorumError.NONE, "", 3, 0);
        var id = new vn.huyqt.logbroker.controller.snapshot.SnapshotId(9, 2, new UUID(0, 4));
        for (var frame : List.of(
            response((short) 110, new BrokerControlProtocol.RegisterReply(meta, session, 9, 12)),
            response((short) 107, new BrokerControlProtocol.CreateTopicReply(meta, new UUID(0, 5), 12)),
            response((short) 113, new BrokerControlProtocol.ObserverSnapshotReply(meta, id, 0, 3, new byte[] {1,2,3})))) {
            byte[] bytes = QuorumCodec.encode(frame);
            assertEquals(bytes.length, QuorumCodec.encodedSize(frame));
            var decoded = QuorumCodec.decode(bytes, config);
            if (frame.message() instanceof BrokerControlProtocol.ObserverSnapshotReply chunk)
                assertArrayEquals(chunk.chunk(), ((BrokerControlProtocol.ObserverSnapshotReply) decoded.message()).chunk());
            else assertEquals(frame.message(), decoded.message());
        }
    }

    @Test void controlRequestsRoundTripWithExactPreflightSize() throws Exception {
        for (var request : List.of(
            new BrokerControlProtocol.Register(7, session.storageId(), session.incarnationId(), -1,
                new ClusterRecords.Endpoint("localhost", 9092), (short) 2, (short) 2, 2000),
            new BrokerControlProtocol.Heartbeat(session, 1, 9, new UUID(0, 3), false, 2000),
            new BrokerControlProtocol.ObserverFetch(session, 3, 9, 2, 4096, 100),
            new BrokerControlProtocol.ObserverSnapshot(session, 3,
                new vn.huyqt.logbroker.controller.snapshot.SnapshotId(9, 2, new UUID(0, 4)), 0, 65536))) {
            var frame = new Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER,
                QuorumProtocol.operation(request), false, config.identity().clusterId(), 7, 12, new byte[32], request);
            byte[] bytes = QuorumCodec.encode(frame);
            assertEquals(bytes.length, QuorumCodec.encodedSize(frame));
            assertEquals(request, QuorumCodec.decode(bytes, config).message());
            assertTrue(QuorumCodec.preflight(bytes, config) >= 2L * bytes.length);
            assertThrows(IOException.class, () -> QuorumCodec.decode(Arrays.copyOf(bytes, bytes.length - 1), config));
        }
    }

    @Test void brokerCannotVoteAndOverlappingBrokerIdDoesNotImplyVoter() {
        var frame = new Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER, (short) 101,
            false, config.identity().clusterId(), 1, 7, new byte[32], new Vote(1, 0, 0));
        assertFalse(BrokerControlProtocol.allowed(frame));
    }

    @Test void structuredRefusalRetainsCommittedRevisionAndRecoveryId() throws Exception {
        var meta = new ReplyMeta(QuorumError.NONE, "", 3, 0);
        var reply = new BrokerControlProtocol.HeartbeatReply(meta, BrokerControlProtocol.SessionStatus.STALE_SESSION,
            9, 10, 12, new UUID(0, 3));
        var frame = response((short) 111, reply);
        byte[] bytes = QuorumCodec.encode(frame);
        assertEquals(bytes.length, QuorumCodec.encodedSize(frame));
        assertEquals(reply, QuorumCodec.decode(bytes, config).message());
    }

    @Test void observerResponseRejectsUncommittedTailAndSnapshot() throws Exception {
        var meta = new ReplyMeta(QuorumError.NONE, "", 3, 0);
        var batch = new vn.huyqt.logbroker.controller.log.QuorumBatch(9,
            List.of(new vn.huyqt.logbroker.controller.log.QuorumEntry.ReadBarrier(2)));
        var good = response((short) 112, new BrokerControlProtocol.ObserverFetchReply(meta, 10, new FetchData(List.of(batch))));
        assertEquals(QuorumCodec.encode(good).length, QuorumCodec.encodedSize(good));
        assertEquals(good.message(), QuorumCodec.decode(QuorumCodec.encode(good), config).message());
        var bad = response((short) 112, new BrokerControlProtocol.ObserverFetchReply(meta, 9, new FetchData(List.of(batch))));
        assertThrows(IOException.class, () -> QuorumCodec.decode(QuorumCodec.encode(bad), config));
    }

    @Test void fullImageAndMembershipRepliesPreflightAndDecode() throws Exception {
        var meta = new ReplyMeta(QuorumError.NONE, "", 3, 0);
        var status = new vn.huyqt.logbroker.controller.consensus.QuorumStatus(0,
            vn.huyqt.logbroker.controller.consensus.QuorumStatus.Role.LEADER, 3, 0, new UUID(0, 4),
            12, 12, 12, 12, 0, true, Map.of(0, 12L), "");
        var described = response((short) 106, new BrokerControlProtocol.DescribeReply(meta, status,
            config.identity().voters(), config.identity().voterHash()));
        byte[] bytes = QuorumCodec.encode(described);
        assertEquals(bytes.length, QuorumCodec.encodedSize(described));
        var decoded = (BrokerControlProtocol.DescribeReply) QuorumCodec.decode(bytes, config).message();
        assertArrayEquals(config.identity().voterHash(), decoded.voterHash());
        assertEquals(config.identity().voters(), decoded.voters());
        var image = new vn.huyqt.logbroker.controller.metadata.MetadataImage(12, List.of(), (short) 2, Map.of(), Map.of());
        var read = response((short) 108, new BrokerControlProtocol.MetadataReply(meta, Consistency.LINEARIZABLE, 0, 12, image));
        bytes = QuorumCodec.encode(read);
        assertEquals(bytes.length, QuorumCodec.encodedSize(read));
        assertEquals(read.message(), QuorumCodec.decode(bytes, config).message());
    }

    private Frame response(short op, Reply reply) {
        return new Frame((short) 2, BrokerControlProtocol.SenderRole.VOTER, op, true,
            config.identity().clusterId(), 0, 12, config.identity().voterHash(), reply);
    }
}
