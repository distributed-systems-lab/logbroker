package vn.huyqt.logbroker.controller.protocol;

import static org.junit.jupiter.api.Assertions.*;
import static vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;

class QuorumCodecTest {
  private final ControllerConfig config =
      ControllerConfig.defaults(ControllerTestSupport.identity(0));

  private Frame frame(short operation, boolean reply, Message message) {
    return new Frame(operation, reply, new UUID(0, 1), -1, 7, new byte[32], message);
  }

  @Test
  void describeEnvelopeMatchesIndependentLiteralCrcVector() throws Exception {
    String hex =
        "00000045006a00010000000000000000000000000000000001ffffffff0000000000000007"
            + "0000000000000000000000000000000000000000000000000000000000000000054ecf7f";
    byte[] golden = HexFormat.of().parseHex(hex);
    assertArrayEquals(golden, QuorumCodec.encode(frame((short) 106, false, new DescribeQuorum())));
    assertInstanceOf(DescribeQuorum.class, QuorumCodec.decode(golden, config).message());
  }

  @Test
  void allRequestAndSuccessPayloadsRoundTrip() throws Exception {
    var id = new SnapshotId(9, 3, new UUID(0, 99));
    var meta = new ReplyMeta(QuorumError.NONE, "", 3, 0);
    var view = new MetadataView(Consistency.LOCAL, 0, 3, 0, 9, 9, List.of());
    var status =
        new QuorumStatus(
            0,
            QuorumStatus.Role.LEADER,
            3,
            0,
            new UUID(0, 9),
            9,
            9,
            9,
            9,
            9,
            true,
            Map.of(0, 9L, 1, 9L),
            "");
    List<Frame> frames =
        List.of(
            frame((short) 101, false, new Vote(3, 2, 9)),
            frame((short) 101, true, new VoteReply(meta, true)),
            frame((short) 102, false, new BeginQuorumEpoch(3)),
            frame((short) 102, true, new EpochReply(meta)),
            frame((short) 103, false, new EndQuorumEpoch(3)),
            frame((short) 103, true, new EpochReply(meta)),
            frame((short) 104, false, new QuorumFetch(3, 9, 2, 1024, 100, 7)),
            frame(
                (short) 104,
                true,
                new QuorumFetchReply(
                    meta,
                    7,
                    9,
                    new FetchData(
                        List.of(new QuorumBatch(9, List.of(new QuorumEntry.ReadBarrier(3))))))),
            frame((short) 104, true, new QuorumFetchReply(meta, 7, 9, new Divergence(2, 8))),
            frame((short) 104, true, new QuorumFetchReply(meta, 7, 9, new SnapshotRequired(id))),
            frame((short) 105, false, new FetchSnapshot(3, id, 0, 256)),
            frame((short) 105, true, new FetchSnapshotReply(meta, id, 0, 3, new byte[] {1, 2, 3})),
            frame((short) 106, true, new DescribeQuorumReply(meta, status)),
            frame((short) 107, false, new CreateTopic("orders", 3, 1000)),
            frame((short) 107, true, new CreateTopicReply(meta, new UUID(0, 3))),
            frame((short) 108, false, new ReadMetadata(1000)),
            frame((short) 108, true, new MetadataReply(meta, view)),
            frame((short) 109, false, new ReadLocalMetadata()),
            frame((short) 109, true, new MetadataReply(meta, view)));
    for (var expected : frames) {
      assertEquals(QuorumCodec.encode(expected).length, QuorumCodec.encodedSize(expected));
      var decoded = QuorumCodec.decode(QuorumCodec.encode(expected), config);
      assertEquals(expected.operation(), decoded.operation());
      assertEquals(expected.requestId(), decoded.requestId());
      assertArrayEquals(QuorumCodec.encode(expected), QuorumCodec.encode(decoded));
      assertEquals(expected.message().getClass(), decoded.message().getClass());
    }
  }

  @Test
  void failureHasNoSuccessPayloadAndErrorNumbersStayStable() throws Exception {
    var expected =
        frame(
            (short) 107,
            true,
            new Failure(new ReplyMeta(QuorumError.NOT_LEADER, "redirect", 4, 2)));
    var decoded = QuorumCodec.decode(QuorumCodec.encode(expected), config);
    assertEquals(14, ((Failure) decoded.message()).meta().error().number());
    assertEquals(6, QuorumError.TOPIC_ALREADY_EXISTS.number());
  }
}
