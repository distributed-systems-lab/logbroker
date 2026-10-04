package vn.huyqt.logbroker.controller.log;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

class QuorumEntryCodecTest {
    @Test
    void barrierHasVersionTypeAndEpochWithNoTrailingPayload() throws Exception {
        byte[] golden = HexFormat.of().parseHex("0001030000000000000007");
        assertArrayEquals(golden, QuorumEntryCodec.encode(new QuorumEntry.ReadBarrier(7)));
        assertEquals(new QuorumEntry.ReadBarrier(7), QuorumEntryCodec.decode(golden));
    }

    @Test
    void roundTripsTopicAndLeaderChange() throws Exception {
        for (var entry :
                List.of(
                        new QuorumEntry.LeaderChange(2, 1),
                        new QuorumEntry.Topic(2, new TopicCreated(new UUID(0, 9), "orders", 3))))
            assertEquals(entry, QuorumEntryCodec.decode(QuorumEntryCodec.encode(entry)));
    }

    @Test
    void rejectsUnsupportedVersionTrailingBytesAndInvalidEpoch() {
        for (String hex :
                List.of(
                        "0003030000000000000007",
                        "000103000000000000000700",
                        "000103ffffffffffffffff",
                        "0001040000000000000007"))
            assertThrows(
                    IOException.class, () -> QuorumEntryCodec.decode(HexFormat.of().parseHex(hex)));
    }

    @Test
    void batchRejectsMixedEpochEmptyAndOffsetOverflow() {
        assertThrows(IllegalArgumentException.class, () -> new QuorumBatch(0, List.of()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new QuorumBatch(
                                0,
                                List.of(
                                        new QuorumEntry.ReadBarrier(1),
                                        new QuorumEntry.ReadBarrier(2))));
        assertThrows(
                IllegalArgumentException.class,
                () -> new QuorumBatch(Long.MAX_VALUE, List.of(new QuorumEntry.ReadBarrier(1))));
    }
}
